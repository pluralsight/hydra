package hydra.kafka.algebras

import cats.effect.concurrent.Ref
import cats.effect.{Async, Concurrent, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.syntax.all._
import fs2.kafka._
import hydra.core.protocol._
import hydra.kafka.util.KafkaUtils.TopicDetails
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

  def live[F[_]: Sync: ConcurrentEffect: ContextShift: Timer](
      bootstrapServers: String,
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

      override def getConsumerGroupOffsets(consumerGroup: String): F[Map[TopicAndPartition, Offset]] =
        getAdminClientResource.use(_.listConsumerGroupOffsets(consumerGroup)
          .partitionsToOffsetAndMetadata.map(_.map(r => TopicAndPartition(r._1) -> Offset(r._2))))

      override def getLatestOffsets(topic: TopicName): F[Map[TopicAndPartition, Offset]] =
        getConsumerResource.use { consumer =>
          consumer.partitionsFor(topic).map(_.map(p => new TopicPartition(p.topic, p.partition))).flatMap { topicPartition =>
            consumer.endOffsets(topicPartition.toSet).map(_.map(in => TopicAndPartition(in._1) -> Offset(in._2)))
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
        consumerResource[F, String, String](
          ConsumerSettings.apply(des, des).withBootstrapServers(bootstrapServers)
        )
      }

      private def getAdminClientResource: Resource[F, KafkaAdminClient[F]] = {
        adminClientResource(
          AdminClientSettings.apply.withBootstrapServers(bootstrapServers)
        )
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
        ref.update(_ - name)

      // This is intentionally unimplemented. This test class has no way of obtaining this offset information.
      override def getConsumerGroupOffsets(consumerGroup: String): F[Map[TopicAndPartition, Offset]] = ???
      // This is intentionally unimplemented. This test class has no way of obtaining this offset information.
      override def getLatestOffsets(topic: TopicName): F[Map[TopicAndPartition, Offset]] = ???
      // This is intentionally unimplemented. This test class has no way of obtaining this offset information.
      override def getConsumerLag(topic: TopicName, consumerGroup: String): F[Map[TopicAndPartition, LagOffsets]] = ???
    }
  }

}
