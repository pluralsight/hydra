package hydra.kafka.algebras

import java.time.Instant
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Timer}
import cats.implicits._
import fs2.kafka._
import hydra.avro.registry.SchemaRegistry
import hydra.kafka.algebras.ConsumerGroupsAlgebra.{Consumer, ConsumerTopics, Topic, TopicConsumers}
import hydra.kafka.algebras.KafkaAdminAlgebra.{LagOffsets, Offset, TopicAndPartition}
import hydra.kafka.algebras.KafkaClientAlgebra.Record
import hydra.kafka.model.TopicConsumer
import hydra.kafka.model.TopicConsumer.{TopicConsumerKey, TopicConsumerValue}
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.util.ConsumerGroupsOffsetConsumer
import io.chrisdavenport.log4cats.Logger
import org.apache.avro.generic.GenericRecord

trait ConsumerGroupsAlgebra[F[_]] {
  def getConsumersForTopic(topicName: String): F[TopicConsumers]
  def getTopicsForConsumer(consumerGroupName: String): F[ConsumerTopics]
  def getAllConsumers: F[List[ConsumerTopics]]
  def getAllConsumersByTopic: F[List[TopicConsumers]]
  def startConsumer: F[Unit]
  def getDetailedConsumerInfo(consumerGroupName: String) : F[List[Topic]]
}

object ConsumerGroupsAlgebra {

  type PartitionOffsetMap = Map[Int, Long]
  final case class PartitionOffset(partition: Int, groupOffset: Offset, largestOffset: Offset, partitionLag: Long)

  final case class TopicConsumers(topicName: String, consumers: List[Consumer])
  final case class Consumer(consumerGroupName: String, lastCommit: Instant)

  final case class ConsumerTopics(consumerGroupName: String, topics: List[Topic])
  final case class Topic(topicName: String, lastCommit: Instant, offsetInformation: Option[List[PartitionOffset]] = None)
  // This is an Optional List so that we don't display information that is not needed, yes it is gross, yes it looks better this way

  def make[F[_]: ContextShift: ConcurrentEffect: Timer: Logger](
                                                                 kafkaInternalTopic: String,
                                                                 dvsConsumersTopic: Subject,
                                                                 consumerOffsetsOffsetsTopicConfig: Subject, // __consumer_offsets is the internal kafka topic we're reading off of
                                                                 bootstrapServers: String,
                                                                 uniquePerNodeConsumerGroup: String,
                                                                 commonConsumerGroup: String,
                                                                 kafkaClientAlgebra: KafkaClientAlgebra[F],
                                                                 kAA: KafkaAdminAlgebra[F],
                                                                 sra: SchemaRegistry[F]): F[ConsumerGroupsAlgebra[F]] = {

    val dvsConsumersStream: fs2.Stream[F, (GenericRecord, Option[GenericRecord], Option[Headers])] = kafkaClientAlgebra.consumeMessages(dvsConsumersTopic.value, uniquePerNodeConsumerGroup, commitOffsets = false)

    for {
      consumerGroupsStorageFacade <- Ref[F].of(ConsumerGroupsStorageFacade.empty)
    } yield new ConsumerGroupsAlgebra[F] {

      override def getConsumersForTopic(topicName: String): F[TopicConsumers] =
        consumerGroupsStorageFacade.get.map(_.getConsumersForTopicName(topicName))

      override def getTopicsForConsumer(consumerGroupName: String): F[ConsumerTopics] =
        consumerGroupsStorageFacade.get.map(_.getTopicsForConsumerGroupName(consumerGroupName))

      override def getAllConsumers: F[List[ConsumerTopics]] =
        consumerGroupsStorageFacade.get.map(_.getAllConsumers)

      override def startConsumer: F[Unit] = {
        for {
          _ <- Concurrent[F].start(consumeDVSConsumersTopicIntoCache(dvsConsumersStream, consumerGroupsStorageFacade))
          _ <- Concurrent[F].start {
            ConsumerGroupsOffsetConsumer.start(kafkaClientAlgebra, kAA, sra, uniquePerNodeConsumerGroup, consumerOffsetsOffsetsTopicConfig, kafkaInternalTopic, dvsConsumersTopic, bootstrapServers, commonConsumerGroup)
          }
        } yield ()
      }

      override def getAllConsumersByTopic: F[List[TopicConsumers]] =
        consumerGroupsStorageFacade.get.map(_.getAllConsumersByTopic)

      override def getDetailedConsumerInfo(consumerGroupName: String): F[List[Topic]] = {
        getTopicsForConsumer(consumerGroupName).flatMap{topicInfo =>
          topicInfo.topics.traverse{topic =>
            kAA.getConsumerLag(topic.topicName, consumerGroupName).map {lag =>
              Topic(topic.topicName, topic.lastCommit, Some(lag.toList.map(a =>
                PartitionOffset(a._1.partition, a._2.group, a._2.latest, a._2.latest.value - a._2.group.value))))
            }
          }
        }
      }
    }
  }

  private def consumeDVSConsumersTopicIntoCache[F[_]: ContextShift: ConcurrentEffect: Timer: Logger](
                                                                                                      dvsConsumersStream: fs2.Stream[F, Record],
                                                                                                      consumerGroupsStorageFacade: Ref[F, ConsumerGroupsStorageFacade]
                                                                                                    ): F[Unit] = {
    dvsConsumersStream.flatMap { case (key, value, _) =>
      fs2.Stream.eval(TopicConsumer.decode[F](key, value).flatMap { case (topicKey, topicValue) =>
        topicValue match {
          case Some(tV) =>
            consumerGroupsStorageFacade.update(_.addConsumerGroup(topicKey, tV))
          case None =>
            consumerGroupsStorageFacade.update(_.removeConsumerGroup(topicKey))
        }
      })
    }.compile.drain
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
    val topics = consumerMap.filterKeys(_.consumerGroupName == consumerGroupName).map(p => Topic(p._1.topicName, p._2.lastCommit)).toList
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

