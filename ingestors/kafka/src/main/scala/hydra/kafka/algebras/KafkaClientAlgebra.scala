package hydra.kafka.algebras

import akka.actor.ActorSelection
import cats.effect.{Async, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import fs2.kafka.{AutoOffsetReset, ConsumerSettings, Deserializer, Serializer, consumerStream}
import hydra.core.protocol._
import hydra.core.transport.AckStrategy
import hydra.kafka.algebras.KafkaAdminAlgebra.{PublishError, TopicName}
import hydra.kafka.producer.{AvroKeyRecord, KafkaRecord}

import scala.concurrent.duration._

trait KafkaClientAlgebra[F[_], K, V] {
  /**
    * Publishes the Hydra record to Kafka
    * @param record - the hydra record that is to be ingested in Kafka
    * @return Either[PublishError, Unit] - Unit is returned upon success, PublishError on failure.
    */
  def publishMessage(
    record: (K, V)
  ): F[Either[PublishError, Unit]]


  /**
    * Consume the Hydra record from Kafka
    * @param topicName - topic name to consume
    * @param consumerGroup - group id for consume
    * @return Stream that results in tupled K and V
    */
  def consumeMessages(
     topicName: TopicName,
     consumerGroup: String
   ): fs2.Stream[F, (K,V)]

}

object KafkaClientAlgebra {
  def live[F[_]: Async: ContextShift: ConcurrentEffect: Timer, K: Deserializer[F, *]: Serializer[F, *], V: Deserializer[F, *]: Serializer[F, *]](
      ingestActor: ActorSelection,
      bootstrapServers: String
  ): F[KafkaClientAlgebra[F, K, V]] = Sync[F].delay {
    new KafkaClientAlgebra[F, K, V] {
      override def publishMessage(
                                   record: (K, V)
                                 ): F[Either[PublishError, Unit]] = {
        import akka.pattern.ask

        implicit val timeout: akka.util.Timeout = akka.util.Timeout(1.second)
        val ingestRecord = Async.fromFuture(
          Sync[F].delay {
        (ingestActor ? Ingest(record, AckStrategy.Replicated))
              .mapTo[IngestorStatus]
          )
          }
        val ingestionResult: F[Unit] = ingestRecord.flatMap {
          case IngestorCompleted => Async[F].unit
          case IngestorError(error) =>
            Async[F].raiseError(PublishError.Failed(error))
          case IngestorTimeout => Async[F].raiseError(PublishError.Timeout)
          case otherStatus =>
            Async[F].raiseError(PublishError.UnexpectedResponse(otherStatus))
        }

        ingestionResult.attemptNarrow[PublishError]
      }

      override def consumeMessages(topicName: TopicName, consumerGroup: String): fs2.Stream[F, (K, V)] = {
        val consumerSettings = ConsumerSettings[F, K, V]
          .withAutoOffsetReset(AutoOffsetReset.Earliest)
          .withBootstrapServers(bootstrapServers)
          .withGroupId(consumerGroup)
        consumerStream(consumerSettings)
          .evalTap(_.subscribeTo(topicName))
          .flatMap(_.stream)
          .map { committable =>
            val r = committable.record
            (r.key, r.value)
          }
      }
    }
  }

  def test[F[_]: Sync, K, V]: F[KafkaClientAlgebra[F, K, V]] = Sync[F].delay {
    new KafkaClientAlgebra[F, K, V] {
      override def publishMessage(
                                   record: KafkaRecord[K, V]
                                 ): F[Either[PublishError, Unit]] =
        Sync[F].pure(Right(()))

      override def consumeMessages(topicName: TopicName, consumerGroup: String): fs2.Stream[F, (K, V)] = {
        fs2.Stream.empty
      }
    }
  }

}
