package hydra.kafka.algebras

import java.util.UUID

import akka.actor.ActorSelection
import cats.effect.{Async, Concurrent, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import fs2.kafka.{AutoOffsetReset, ConsumerSettings, Deserializer, ProducerRecord, ProducerRecords, ProducerSettings, Serializer, consumerStream}
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


  private def getProducerQueue[F[_]: ConcurrentEffect: Async: ContextShift, K: Serializer[F, *], V: Serializer[F, *]](bootstrapServers: String): F[fs2.concurrent.Queue[F, (K, V, TopicName)]] = {
    import fs2.kafka._
    val producerSettings =
      ProducerSettings[F, K, V]
        .withBootstrapServers(bootstrapServers)
    for {
      queue <- fs2.concurrent.Queue.unbounded[F, (K, V, TopicName)]
      _ <- Concurrent[F].start(queue.dequeue.map { record =>
        ProducerRecords.one(ProducerRecord(record._3, record._1, record._2), UUID.randomUUID)
      }.through(produce(producerSettings)).map(i => i.passthrough).compile.drain)
    } yield queue
  }

  def live[F[_]: Async: ContextShift: ConcurrentEffect: Timer, K: Deserializer[F, *]: Serializer[F, *], V: Deserializer[F, *]: Serializer[F, *]](
      ingestActor: ActorSelection,
      bootstrapServers: String
  ): F[KafkaClientAlgebra[F, K, V]] = getProducerQueue[F, K, V](bootstrapServers).map { queue =>

    new KafkaClientAlgebra[F, K, V] {
      override def publishMessage(
                                   record: (K, V),
                                   topicName: TopicName
                                 ): F[Either[PublishError, Unit]] = {
        queue.enqueue1((record._1, record._2, topicName))
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
