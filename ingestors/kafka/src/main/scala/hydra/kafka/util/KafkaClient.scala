package hydra.kafka.util

import akka.actor.ActorSelection
import cats.Monad
import cats.effect.concurrent.Ref
import cats.effect.{Async, Concurrent, ContextShift, Resource, Sync}
import cats.implicits._
import fs2.kafka._
import hydra.core.protocol.{Ingest, IngestorCompleted, IngestorError, IngestorStatus, IngestorTimeout}
import hydra.core.transport.AckStrategy
import hydra.kafka.util.KafkaUtils.TopicDetails
import org.apache.kafka.clients.admin.NewTopic
import hydra.kafka.producer.KafkaRecord

import scala.concurrent.duration._
import scala.util.control.NoStackTrace
import com.fasterxml.jackson.module.scala.deser.`package`.overrides

trait KafkaClient[F[_]]  {
  import KafkaClient._

  def describeTopic(name: TopicName): F[Option[Topic]]

  def getTopicNames: F[List[TopicName]]

  def createTopic(name: TopicName, details: TopicDetails): F[Unit]

  def publishMessage[K, V](record: KafkaRecord[K, V]): F[Either[PublishError, Unit]]

}

object KafkaClient {

  type TopicName = String
  final case class Topic(name: TopicName, numberPartitions: Int)

  sealed abstract class PublishError extends Exception with Product with Serializable
  final case object PublishTimeoutError extends PublishError with NoStackTrace
  final case class PublishFailed(ingestorResponse: IngestorStatus) extends PublishError

  def live[F[_]: Async: Concurrent: ContextShift](bootstrapServers: String, ingestActor: ActorSelection): F[KafkaClient[F]] = Sync[F].delay {
    new KafkaClient[F] {

      override def describeTopic(name: TopicName): F[Option[Topic]] = {
        getAdminClientResource.use(_.describeTopics(name :: Nil)).map(_.headOption.map(_._2).map { td =>
          Topic(td.name(), td.partitions().size())
        })
      }

      override def getTopicNames: F[List[TopicName]] = getAdminClientResource.use(_.listTopics.names.map(_.toList))

      override def createTopic(name: TopicName, d: TopicDetails): F[Unit] = {
        import scala.collection.JavaConverters._
        val newTopic = new NewTopic(name, d.numPartitions, d.replicationFactor).configs(d.configs.asJava)
        getAdminClientResource.use(_.createTopic(newTopic))
      }

      override def publishMessage[K, V](record: KafkaRecord[K, V]): F[Either[PublishError, Unit]] = {
        import akka.pattern.ask

        implicit val timeout: akka.util.Timeout = akka.util.Timeout(1.second)
        val ingestRecord = Async.fromFuture(Sync[F].delay((ingestActor ? Ingest(record, AckStrategy.Replicated)).mapTo[IngestorStatus]))
        val ingestionResult: F[Unit] = ingestRecord.flatMap {
          case IngestorCompleted => Async[F].unit
          case IngestorError(error) => Async[F].raiseError(error)
          case IngestorTimeout => Async[F].raiseError(PublishTimeoutError)
          case otherStatus => Async[F].raiseError(PublishFailed(otherStatus))
        }

        ingestionResult.attemptNarrow[PublishError]
      }



      private def getAdminClientResource: Resource[F, KafkaAdminClient[F]] = {
        adminClientResource(AdminClientSettings.apply.withBootstrapServers(bootstrapServers))
      }
    }
  }

  def test[F[_]: Sync]: F[KafkaClient[F]] = Ref[F].of(Map[TopicName, Topic]()).flatMap(getTestKafkaClient[F])

  private[this] def getTestKafkaClient[F[_]: Sync](ref: Ref[F, Map[TopicName, Topic]]): F[KafkaClient[F]] = Sync[F].delay {
    new KafkaClient[F] {
      override def describeTopic(name: TopicName): F[Option[Topic]] = ref.get.map(_.get(name))

      override def getTopicNames: F[List[TopicName]] = ref.get.map(_.keys.toList)

      override def createTopic(name: TopicName, details: TopicDetails): F[Unit] = {
        val entry = name -> Topic(name, details.numPartitions)
        ref.update(old => old + entry)
      }

      override def publishMessage[K, V](record: KafkaRecord[K,V]): F[Either[PublishError, Unit]] =
        Sync[F].pure(Right(()))
      
    }
  }

}
