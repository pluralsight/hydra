package hydra.kafka.algebras

import cats.ApplicativeError

import java.time.Instant
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, IO, Timer}
import cats.implicits._
import hydra.avro.registry.SchemaRegistry
import hydra.kafka.algebras.ConsumerGroupsAlgebra._
import hydra.kafka.algebras.KafkaClientAlgebra.Record
import hydra.kafka.algebras.RetryableFs2Stream.RetryPolicy.Infinite
import hydra.kafka.algebras.RetryableFs2Stream._
import hydra.kafka.model.TopicConsumer
import hydra.kafka.model.TopicConsumer.{TopicConsumerKey, TopicConsumerValue}
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.serializers.TopicMetadataV2Parser.IntentionallyUnimplemented
import hydra.kafka.util.ConsumerGroupsOffsetConsumer
import org.apache.avro.generic.GenericRecord
import org.typelevel.log4cats.Logger

trait ConsumerGroupsAlgebra[F[_]] {
  def getConsumersForTopic(topicName: String): F[TopicConsumers]

  def getTopicsForConsumer(consumerGroupName: String): F[ConsumerTopics]

  def getAllConsumers: F[List[ConsumerTopics]]

  def getAllConsumersByTopic: F[List[TopicConsumers]]

  def startConsumer: F[Unit]

  def getDetailedConsumerInfo(consumerGroupName: String): F[List[DetailedConsumerGroup]]

  def getConsumerActiveState(consumerGroupName: String): F[String]

  def consumerGroupIsActive(str: String): F[(Boolean, String)]

  def getUniquePerNodeConsumerGroup: String
}

final case class TestConsumerGroupsAlgebra(consumerGroupMap: Map[TopicConsumerKey, (TopicConsumerValue, String)]) extends ConsumerGroupsAlgebra[IO] {

  def addConsumerGroup(key: TopicConsumerKey, value: TopicConsumerValue, state: String): TestConsumerGroupsAlgebra = {
    this.copy(this.consumerGroupMap + (key -> (value, state)))
  }

  def removeConsumerGroup(key: TopicConsumerKey): TestConsumerGroupsAlgebra = {
    this.copy(this.consumerGroupMap - key)
  }

  override def getConsumersForTopic(topicName: String): IO[TopicConsumers] = {
    val consumerGroups = consumerGroupMap.filterKeys(_.topicName == topicName).map(p =>
      Consumer(p._1.consumerGroupName, p._2._1.lastCommit, state = Some(p._2._2))).toList
    IO.pure(TopicConsumers(topicName, consumerGroups))
  }

  override def getTopicsForConsumer(consumerGroupName: String): IO[ConsumerTopics] = {
    val topics = consumerGroupMap.filterKeys(_.consumerGroupName == consumerGroupName)
      .map(p => DetailedConsumerGroup(p._1.topicName, p._1.consumerGroupName, p._2._1.lastCommit, state = Some(p._2._2))).toList
    IO.pure(ConsumerTopics(consumerGroupName, topics))
  }

  override def getAllConsumers: IO[List[ConsumerTopics]] =
    consumerGroupMap.keys.map(_.consumerGroupName).toList.traverse(getTopicsForConsumer)

  override def getAllConsumersByTopic: IO[List[TopicConsumers]] = consumerGroupMap.keys.map(_.topicName).toList.traverse(getConsumersForTopic)

  override def startConsumer: IO[Unit] = throw IntentionallyUnimplemented

  override def getDetailedConsumerInfo(consumerGroupName: String): IO[List[DetailedConsumerGroup]] = {
    getTopicsForConsumer(consumerGroupName).flatMap { topicInfo =>
      topicInfo.topics.traverse { topic =>
        IO.pure(DetailedConsumerGroup(topic.topicName, consumerGroupName, topic.lastCommit,
          List(PartitionOffset(0, 0, 0, 0)), Some(0), Some(consumerGroupMap.get(TopicConsumerKey(topic.topicName, consumerGroupName))).get.map(_._2)))
      }
    }
  }

  override def getConsumerActiveState(consumerGroupName: String): IO[String] = {
    IO.pure(consumerGroupMap.keys.map { keys =>
      if (keys.consumerGroupName == consumerGroupName) consumerGroupMap(keys)._2 else "Unknown"
    }.head)
  }

  override def consumerGroupIsActive(str: String): IO[(Boolean, String)] = {
    getConsumerActiveState(str).map(state => (state == "Stable", str))
  }

  override def getUniquePerNodeConsumerGroup: String = "uniquePerNodeConsumerGroup"

}

object TestConsumerGroupsAlgebra {
  def empty: TestConsumerGroupsAlgebra = TestConsumerGroupsAlgebra(Map.empty[TopicConsumerKey, (TopicConsumerValue, String)])
}

object ConsumerGroupsAlgebra {

  type PartitionOffsetMap = Map[Int, Long]

  final case class PartitionOffset(partition: Int, groupOffset: Long, largestOffset: Long, partitionLag: Long)

  final case class TopicConsumers(topicName: String, consumers: List[Consumer])

  final case class DetailedTopicConsumers(topicName: String, consumers: List[DetailedConsumerGroup])

  final case class Consumer(consumerGroupName: String, lastCommit: Instant, state: Option[String] = None)

  final case class ConsumerTopics(consumerGroupName: String, topics: List[DetailedConsumerGroup])

  final case class DetailedConsumerGroup(topicName: String, consumerGroupName: String, lastCommit: Instant,
                                         offsetInformation: List[PartitionOffset] = List.empty, totalLag: Option[Long] = None, state: Option[String] = None)

  def make[F[_] : ContextShift : ConcurrentEffect : Timer : Logger](
                                                                     kafkaInternalTopic: String,
                                                                     dvsConsumersTopic: Subject,
                                                                     consumerOffsetsOffsetsTopicConfig: Subject, // __consumer_offsets is the internal kafka topic we're reading off of
                                                                     bootstrapServers: String,
                                                                     uniquePerNodeConsumerGroup: String,
                                                                     commonConsumerGroup: String,
                                                                     kafkaClientAlgebra: KafkaClientAlgebra[F],
                                                                     kAA: KafkaAdminAlgebra[F],
                                                                     sra: SchemaRegistry[F]): F[ConsumerGroupsAlgebra[F]] = {

    val dvsConsumersStream: fs2.Stream[F, Record] = {
      kafkaClientAlgebra.consumeSafelyMessages(dvsConsumersTopic.value, uniquePerNodeConsumerGroup, commitOffsets = false)
        //Ignore records with errors
        .collect { case Right(value) => value }
    }

    for {
      consumerGroupsStorageFacade <- Ref[F].of(ConsumerGroupsStorageFacade.empty)
    } yield new ConsumerGroupsAlgebra[F] {

      override def getConsumersForTopic(topicName: String): F[TopicConsumers] =
        consumerGroupsStorageFacade.get.flatMap(a => addStateToTopicConsumers(a.getConsumersForTopicName(topicName)))

      private def addStateToTopicConsumers(topicConsumers: TopicConsumers): F[TopicConsumers] = {
        val detailedF: F[List[Consumer]] = topicConsumers.consumers.traverse { consumer =>
          val fState = getConsumerActiveState(consumer.consumerGroupName)
          fState.map { state =>
            Consumer(consumer.consumerGroupName,
              consumer.lastCommit, state = Some(state))
          }
        }
        detailedF.map { detailed =>
          TopicConsumers(topicConsumers.topicName, detailed)
        }
      }

      override def getTopicsForConsumer(consumerGroupName: String): F[ConsumerTopics] =
        consumerGroupsStorageFacade.get.map(_.getTopicsForConsumerGroupName(consumerGroupName))

      override def getAllConsumers: F[List[ConsumerTopics]] =
        consumerGroupsStorageFacade.get.map(_.getAllConsumers)

      override def startConsumer: F[Unit] = {
        for {
          _ <- Concurrent[F].start(consumeDVSConsumersTopicIntoCache(dvsConsumersStream, consumerGroupsStorageFacade))
          _ <- Concurrent[F].start {
            ConsumerGroupsOffsetConsumer.start(kafkaClientAlgebra, kAA, sra, uniquePerNodeConsumerGroup,
              consumerOffsetsOffsetsTopicConfig, kafkaInternalTopic, dvsConsumersTopic, bootstrapServers, commonConsumerGroup)
          }
        } yield ()
      }

      override def getAllConsumersByTopic: F[List[TopicConsumers]] =
        consumerGroupsStorageFacade.get.flatMap(a => a.getAllConsumersByTopic.traverse(b => addStateToTopicConsumers(b)))

      override def getDetailedConsumerInfo(consumerGroupName: String): F[List[DetailedConsumerGroup]] = {
        getTopicsForConsumer(consumerGroupName).flatMap { topicInfo =>
          topicInfo.topics.traverse { topic =>
            getConsumerActiveState(consumerGroupName).flatMap { state =>
              kAA.getConsumerLag(topic.topicName, consumerGroupName).map { lag =>
                DetailedConsumerGroup(topic.topicName, consumerGroupName, topic.lastCommit, lag.toList.map(a =>
                  PartitionOffset(a._1.partition, a._2.group.value, a._2.latest.value, a._2.latest.value - a._2.group.value)), lag.values.map(v => v.latest.value - v.group.value).sum.some, Some(state))
              }
            }
          }
        }
      }

      override def getConsumerActiveState(consumerGroupName: String): F[String] = {
        kAA.describeConsumerGroup(consumerGroupName).map {
          case Some(value) => value.state().toString
          case None => "Unknown"
        }
      }

      override def consumerGroupIsActive(str: String): F[(Boolean, String)] = {
        getConsumerActiveState(str).map(state => (state == "Stable", str))
      }

      override def getUniquePerNodeConsumerGroup: String = uniquePerNodeConsumerGroup
    }
  }

  private def consumeDVSConsumersTopicIntoCache[F[_] : ContextShift : ConcurrentEffect : Timer : Logger](
                                                                                                          dvsConsumersStream: fs2.Stream[F, Record],
                                                                                                          consumerGroupsStorageFacade: Ref[F, ConsumerGroupsStorageFacade]
                                                                                                        ): F[Unit] = {
    val errorMessage = "Error in ConsumergroupsAlgebra consumer"

    dvsConsumersStream.evalTap { case (key, value, _) =>
      TopicConsumer.decode[F](key, value).flatMap {
        case (topicKey, topicValue) =>
          topicValue match {
            case Some(tV) =>
              consumerGroupsStorageFacade.update(_.addConsumerGroup(topicKey, tV))
            case None =>
              consumerGroupsStorageFacade.update(_.removeConsumerGroup(topicKey))
          }
      }.recoverWith {
        case e => Logger[F].error(e)(errorMessage)
      }
    }
      .makeRetryable(Infinite)(errorMessage)
      .compile.drain
  }
}

private case class ConsumerGroupsStorageFacade(consumerMap: Map[TopicConsumerKey, TopicConsumerValue]) {
  def addConsumerGroup(key: TopicConsumerKey, value: TopicConsumerValue): ConsumerGroupsStorageFacade =
    this.copy(this.consumerMap + (key -> value))

  def removeConsumerGroup(key: TopicConsumerKey): ConsumerGroupsStorageFacade =
    this.copy(this.consumerMap - key)

  def getConsumersForTopicName(topicName: String): TopicConsumers = {
    val consumerGroups = consumerMap.filterKeys(_.topicName == topicName).map(p => Consumer(p._1.consumerGroupName, p._2.lastCommit)).toList
    TopicConsumers(topicName, consumerGroups)
  }

  def getTopicsForConsumerGroupName(consumerGroupName: String): ConsumerTopics = {
    val topics = consumerMap.filterKeys(_.consumerGroupName == consumerGroupName)
      .map(p => DetailedConsumerGroup(p._1.topicName, p._1.consumerGroupName, p._2.lastCommit)).toList
    ConsumerTopics(consumerGroupName, topics)
  }

  def getAllConsumers: List[ConsumerTopics] = {
    consumerMap.keys.map(_.consumerGroupName).toSet.map(getTopicsForConsumerGroupName).toList
  }

  def getAllConsumersByTopic: List[TopicConsumers] = {
    consumerMap.keys.map(_.topicName).toSet.map(getConsumersForTopicName).toList
  }
}

private object ConsumerGroupsStorageFacade {
  def empty: ConsumerGroupsStorageFacade = ConsumerGroupsStorageFacade(Map.empty)
}

