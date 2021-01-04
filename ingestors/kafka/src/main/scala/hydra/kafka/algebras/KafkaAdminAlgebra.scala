package hydra.kafka.algebras

import akka.actor.Status.Success
import cats.data.{NonEmptyChain, NonEmptyList, Validated, ValidatedNec, ValidatedNel}
import cats.effect.concurrent.Ref
import cats.effect.{Async, Concurrent, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.syntax.all._
import fs2.kafka._
import hydra.core.protocol._
import hydra.kafka.util.KafkaUtils.TopicDetails
import io.chrisdavenport.log4cats.Logger
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException

import scala.util.control.NoStackTrace

/**
  * Internal interface to interact with the KafkaAdminClient from FS2 Kafka.
  * Provides a live version for production usage and a test version for integration testing.
  * @tparam F - higher kinded type - polymorphic effect type
  */
trait KafkaAdminAlgebra[F[_]] {
  import KafkaAdminAlgebra._

  /**
    * Retrieves Topic if found in Kafka. Provides minimal detail about the topic.
    * @param name - name of the topic in Kafka
    * @return Option[Topic]
    */
  def describeTopic(name: TopicName): F[Option[Topic]]

  /**
    * Retrieves a list of all TopicName(s) found in Kafka
    * @return List[TopicName]
    */
  def getTopicNames: F[List[TopicName]]

  /**
    * Creates the Topic in Kafka
    * @param name - name of the topic to be created in Kafka
    * @param details - config and settings for the topic
    * @return
    */
  def createTopic(name: TopicName, details: TopicDetails): F[Unit]

  /**
    * Deletes the topic in Kafka
    * @param name - name of the topic in Kafka
    * @return
    */
  def deleteTopic(name: String): F[Unit]

  /**
    * Deletes the topic(s) in Kafka
    * @param topicNames - a list of topic names in Kafka
    * @return
    */
  def deleteTopics(topicNames: List[String]): F[Either[KafkaDeleteTopicErrorList, Unit]]

  /**
    * Fetch the offsets by topic and partition for a given consumer group
    * @param consumerGroup The name of the consumer group you are fetching the offsets for
    * @return Offsets keyed by topic and partition
    */
  def getConsumerGroupOffsets(consumerGroup: String): F[Map[TopicAndPartition, Offset]]

  /**
    * Fetch the latest offsets for a given topic
    * @param topic name of the topic to get the last offsets for
    * @return offsets by partition and topic
    */
  def getLatestOffsets(topic: TopicName): F[Map[TopicAndPartition, Offset]]

  /**
    * Returns the lag for a given consumer on a given topic
    * @param topic Name of the topic you want the lag for
    * @param consumerGroup Name of the consumer group you want the lag for
    * @return The latest and group offsets by topic and partition
    */
  def getConsumerLag(topic: TopicName, consumerGroup: String): F[Map[TopicAndPartition, LagOffsets]]

}

object KafkaAdminAlgebra {

  type TopicName = String
  final case class Topic(name: TopicName, numberPartitions: Int)

  final case class TopicAndPartition(topic: String, partition: Int)
  object TopicAndPartition {
    def apply(t: TopicPartition): TopicAndPartition =
      new TopicAndPartition(t.topic, t.partition)
  }
  final case class Offset(value: Long) extends AnyVal
  object Offset {
    def apply(o: OffsetAndMetadata): Offset =
      new Offset(o.offset)
  }
  final case class LagOffsets(latest: Offset, group: Offset)

  final case class KafkaDeleteTopicError(topicName: String, cause: Throwable)
    extends Exception (s"Unable to delete $topicName", cause){
    def errorMessage: String = s"$topicName ${cause.getMessage}"
  }

  final case class KafkaDeleteTopicErrorList(errors: NonEmptyList[KafkaDeleteTopicError])
    extends Exception (s"Topic(s) failed to delete:\n${errors.map(_.errorMessage).toList.mkString("\n")}")

  def live[F[_]: Sync: ConcurrentEffect: ContextShift: Timer: Logger](
      bootstrapServers: String,
      useSsl: Boolean = false
  ): F[KafkaAdminAlgebra[F]] = Sync[F].delay {
    new KafkaAdminAlgebra[F] {

      override def describeTopic(name: TopicName): F[Option[Topic]] = {
        getAdminClientResource
          .use(_.describeTopics(name :: Nil))
          .map(_.headOption.map(_._2).map { td =>
            Topic(td.name(), td.partitions().size())
          })
          .recover {
            case _: UnknownTopicOrPartitionException => None
          }
      }

      override def getTopicNames: F[List[TopicName]] =
        getAdminClientResource.use(_.listTopics.names.map(_.toList))

      override def createTopic(name: TopicName, d: TopicDetails): F[Unit] = {
        import scala.collection.JavaConverters._
        val newTopic = new NewTopic(name, d.numPartitions, d.replicationFactor)
          .configs(d.configs.asJava)
        getAdminClientResource.use(_.createTopic(newTopic))
      }

      override def deleteTopic(name: String): F[Unit] =
        getAdminClientResource.use(_.deleteTopic(name))

      override def deleteTopics(topicNames: List[String]): F[Either[KafkaDeleteTopicErrorList, Unit]] =
        topicNames.traverse{topicName =>
          deleteTopic(topicName).attempt
            .map{
              _.leftMap(
                KafkaDeleteTopicError(topicName, _)
              ).toValidatedNel
            }
        }.map(_.combineAll.toEither.leftMap(errorList => KafkaDeleteTopicErrorList(errorList)))

      override def getConsumerGroupOffsets(consumerGroup: String): F[Map[TopicAndPartition, Offset]] =
        getAdminClientResource.use(_.listConsumerGroupOffsets(consumerGroup)
          .partitionsToOffsetAndMetadata.map(_.map(r => TopicAndPartition(r._1) -> Offset(r._2))))

      override def getLatestOffsets(topic: TopicName): F[Map[TopicAndPartition, Offset]] = {
        getConsumerResource.use { consumerMaybe =>
          Option(consumerMaybe) match {
            case Some(consumer) =>
              consumer.partitionsFor(topic).flatMap { partitionInfosMaybe =>
                Option(partitionInfosMaybe) match {
                  case Some(partitionInfos) =>
                    val p = partitionInfos.map(p => new TopicPartition(p.topic, p.partition))
                    Sync[F].delay(p)
                  case None =>
                    Logger[F].warn(s"PartitionInfoList for topic $topic is null.") *> Sync[F].pure(List[TopicPartition]())
                }
              }.flatMap { topicPartitionMaybe =>
                Option(topicPartitionMaybe) match {
                  case Some(topicPartition) =>
                    consumer.endOffsets(topicPartition.toSet).flatMap { endOffsetsMaybe =>
                      Option(endOffsetsMaybe) match {
                        case Some(endOffsets) =>
                          val endOffsetMap = endOffsets.map(in => TopicAndPartition(in._1) -> Offset(in._2))
                          Sync[F].delay(endOffsetMap)
                        case None =>
                          Logger[F].warn(s"EndOffsets for topic $topic and topicPartition ${topicPartition} is null.") *> Sync[F].pure(Map.empty[KafkaAdminAlgebra.TopicAndPartition, Offset])
                      }
                    }
                  case None =>
                    Logger[F].warn(s"One PartitionInfo for topic $topic is null.") *> Sync[F].pure(Map.empty[KafkaAdminAlgebra.TopicAndPartition, Offset])
                }
              }
            case None =>
              Logger[F].warn(s"Consumer for topic $topic is null.") *> Sync[F].pure(Map.empty[KafkaAdminAlgebra.TopicAndPartition, Offset])
          }
        }
      }

      override def getConsumerLag(topic: TopicName, consumerGroup: String): F[Map[TopicAndPartition, LagOffsets]] = {
        for {
          latest <- getLatestOffsets(topic)
          group <- getConsumerGroupOffsets(consumerGroup)
        } yield {
          latest.map { case (topicAndPartition, latestOffset) =>
            val maybeLag = group.get(topicAndPartition).map(groupOffset => LagOffsets(latestOffset, groupOffset))
            topicAndPartition -> maybeLag.getOrElse(LagOffsets(latestOffset, Offset(0)))
          }
        }
      }

      private def getConsumerResource: Resource[F, KafkaConsumer[F, _, _]] = {
        val des = Deserializer[F, String]
        consumerResource[F, String, String] {
          val s = ConsumerSettings.apply(des, des).withBootstrapServers(bootstrapServers)
          if (useSsl) s.withProperty("security.protocol", "SSL") else s
        }
      }

      private def getAdminClientResource: Resource[F, KafkaAdminClient[F]] = {
        adminClientResource {
          val s = AdminClientSettings.apply.withBootstrapServers(bootstrapServers)
          if (useSsl) s.withProperty("security.protocol", "SSL") else s
        }
      }
    }
  }

  def test[F[_]: Sync]: F[KafkaAdminAlgebra[F]] =
    Ref[F].of(Map[TopicName, Topic]()).flatMap(getTestKafkaClient[F])

  private[this] def getTestKafkaClient[F[_]: Sync](
      ref: Ref[F, Map[TopicName, Topic]]
  ): F[KafkaAdminAlgebra[F]] = Sync[F].delay {
    new KafkaAdminAlgebra[F] {
      override def describeTopic(name: TopicName): F[Option[Topic]] =
        ref.get.map(_.get(name))

      override def getTopicNames: F[List[TopicName]] =
        ref.get.map(_.keys.toList)

      override def createTopic(
          name: TopicName,
          details: TopicDetails
      ): F[Unit] = {
        val entry = name -> Topic(name, details.numPartitions)
        ref.update(old => old + entry)
      }

      override def deleteTopic(name: String): F[Unit] =
        ref.modify(topicMap => if(topicMap.contains(name)) (topicMap - name, None)
        else (topicMap, Some(new UnknownTopicOrPartitionException("Topic does not exist"))))
          .flatMap{
          case Some(e) => Sync[F].raiseError(e)
          case None => Sync[F].unit
        }

      // This is intentionally unimplemented. This test class has no way of obtaining this offset information.
      override def getConsumerGroupOffsets(consumerGroup: String): F[Map[TopicAndPartition, Offset]] = ???
      // This is intentionally unimplemented. This test class has no way of obtaining this offset information.
      override def getLatestOffsets(topic: TopicName): F[Map[TopicAndPartition, Offset]] = ???
      // This is intentionally unimplemented. This test class has no way of obtaining this offset information.
      override def getConsumerLag(topic: TopicName, consumerGroup: String): F[Map[TopicAndPartition, LagOffsets]] = ???

      override def deleteTopics(topicNames: List[String]): F[Either[KafkaDeleteTopicErrorList, Unit]] =
        topicNames.traverse{topicName =>
          deleteTopic(topicName).attempt
            .map{ deleteAttempt =>
              deleteAttempt.leftMap(
                KafkaDeleteTopicError(topicName, _)
              ).toValidatedNel
            }
        }.map(_.combineAll.toEither.leftMap(errorList => KafkaDeleteTopicErrorList(errorList)))
}
  }

}
