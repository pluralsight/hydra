package hydra.kafka.algebras

import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import fs2.kafka._
import hydra.core.protocol._
import hydra.kafka.algebras.KafkaClientAlgebra.{PublishError, TopicName}

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

trait KafkaClientAlgebra[F[_], K, V] {
  /**
    * Publishes the Hydra record to Kafka
    * @param record - the hydra record that is to be ingested in Kafka
    * @return Either[PublishError, Unit] - Unit is returned upon success, PublishError on failure.
    */
  def publishMessage(
    record: (K, V),
    topicName: TopicName
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
   ): fs2.Stream[F, (K, V)]

}

object KafkaClientAlgebra {

  type TopicName = String

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


  private def getProducerQueue[F[_]: ConcurrentEffect: ContextShift,
    K: Serializer[F, *],
    V: Serializer[F, *]]
  (bootstrapServers: String): F[fs2.concurrent.Queue[F, (K, V, TopicName, Deferred[F, Unit])]] = {
    import fs2.kafka._
    val producerSettings =
      ProducerSettings[F, K, V]
        .withBootstrapServers(bootstrapServers)
    for {
      queue <- fs2.concurrent.Queue.unbounded[F, (K, V, TopicName, Deferred[F, Unit])]
      _ <- Concurrent[F].start(queue.dequeue.map { record =>
        ProducerRecords.one(ProducerRecord(record._3, record._1, record._2), record._4)
      }.through(produce(producerSettings)).map(i => i.passthrough.complete(())).compile.drain)
    } yield queue
  }

  def live[F[_]: ContextShift: ConcurrentEffect: Timer, K: Deserializer[F, *]: Serializer[F, *], V: Deserializer[F, *]: Serializer[F, *]](
      bootstrapServers: String
  ): F[KafkaClientAlgebra[F, K, V]] = getProducerQueue[F, K, V](bootstrapServers).map { queue =>

    new KafkaClientAlgebra[F, K, V] {
      override def publishMessage(
                                   record: (K, V),
                                   topicName: TopicName
                                 ): F[Either[PublishError, Unit]] = {

        Deferred[F, Unit].flatMap { d =>
          queue.enqueue1((record._1, record._2, topicName, d)) *>
            Concurrent.timeoutTo[F, Either[PublishError, Unit]](d.get.map(Right(_)), 3.seconds, Sync[F].pure(PublishError.Timeout))
        }
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
      override def publishMessage(record: (K, V), topicName: TopicName): F[Either[PublishError, Unit]] =
        Sync[F].pure(Right(()))

      override def consumeMessages(topicName: TopicName, consumerGroup: String): fs2.Stream[F, (K, V)] =
        fs2.Stream.empty
    }
  }

}
