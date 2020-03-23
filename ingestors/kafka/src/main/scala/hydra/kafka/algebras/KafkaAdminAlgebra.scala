package hydra.kafka.algebras

import cats.effect.concurrent.Ref
import cats.effect.{Async, Concurrent, ContextShift, Resource, Sync}
import cats.implicits._
import fs2.kafka._
import hydra.core.protocol._
import hydra.kafka.util.KafkaUtils.TopicDetails
import org.apache.kafka.clients.admin.NewTopic
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
}

object KafkaAdminAlgebra {

  type TopicName = String
  final case class Topic(name: TopicName, numberPartitions: Int)

  sealed abstract class PublishError(message: String)
      extends Exception(message)
      with Product
      with Serializable

  object PublishError {

    final case object Timeout
        extends PublishError("Timeout while ingesting message.")
        with NoStackTrace

    final case class UnexpectedResponse(ingestorResponse: IngestorStatus)
        extends PublishError(
          s"Unexpected response from ingestor: $ingestorResponse"
        )

    final case class Failed(cause: Throwable)
        extends PublishError(cause.getMessage)
  }

  def live[F[_]: Sync: Concurrent: ContextShift](
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

      private def getAdminClientResource: Resource[F, KafkaAdminClient[F]] = {
        adminClientResource(
          AdminClientSettings.apply.withBootstrapServers(bootstrapServers)
        )
      }
    }
  }

  def test[F[_]: Sync, K, V]: F[KafkaAdminAlgebra[F]] =
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
    }
  }

}
