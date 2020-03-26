package hydra.kafka.algebras

import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import fs2.kafka._
import fs2.kafka.vulcan.KafkaAvroSerializer
import hydra.core.protocol._
import hydra.kafka.algebras.KafkaClientAlgebra.{PublishError, TopicName}
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

trait KafkaClientAlgebra[F[_]] {
  /**
    * Publishes the Hydra record to Kafka
    * @param record - the hydra record that is to be ingested in Kafka
    * @return Either[PublishError, Unit] - Unit is returned upon success, PublishError on failure.
    */
  def publishMessage(
    record: (GenericRecord, GenericRecord),
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
   ): fs2.Stream[F, (GenericRecord, GenericRecord)]

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


  private def getProducerQueue[F[_]: ConcurrentEffect: ContextShift]
  (bootstrapServers: String): F[fs2.concurrent.Queue[F, (GenericRecord, GenericRecord, TopicName, Deferred[F, Unit])]] = {
    import fs2.kafka._
    val genericRecordSerializer = getGenericRecordSerializer
    val producerSettings =
      ProducerSettings(keySerializer = genericRecordSerializer, valueSerializer = genericRecordSerializer)
        .withBootstrapServers(bootstrapServers)
    for {
      queue <- fs2.concurrent.Queue.unbounded[F, (GenericRecord, GenericRecord, TopicName, Deferred[F, Unit])]
      _ <- Concurrent[F].start(queue.dequeue.map { record =>
        ProducerRecords.one(ProducerRecord(record._3, record._1, record._2), record._4)
      }.through(produce(producerSettings)).map(i => i.passthrough.complete(())).compile.drain)
    } yield queue
  }

  def live[F[_]: ContextShift: ConcurrentEffect: Timer](
      bootstrapServers: String
  ): F[KafkaClientAlgebra[F]] = getProducerQueue[F](bootstrapServers).map { queue =>
    val genericRecordDeserializer = getGenericRecordDeserializer
    new KafkaClientAlgebra[F] {
      override def publishMessage(
                                   record: (GenericRecord, GenericRecord),
                                   topicName: TopicName
                                 ): F[Either[PublishError, Unit]] = {

        Deferred[F, Unit].flatMap { d =>
          queue.enqueue1((record._1, record._2, topicName, d)) *>
            Concurrent.timeoutTo[F, Either[PublishError, Unit]](d.get.map(Right(_)), 3.seconds, Sync[F].pure(Left(PublishError.Timeout)))
        }
      }

      override def consumeMessages(topicName: TopicName, consumerGroup: String): fs2.Stream[F, (GenericRecord, GenericRecord)] = {
        val consumerSettings = ConsumerSettings(keyDeserializer = genericRecordDeserializer, valueDeserializer = genericRecordDeserializer)
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

  def test[F[_]: Sync, K, V]: F[KafkaClientAlgebra[F]] = Sync[F].delay {
    new KafkaClientAlgebra[F] {
      override def publishMessage(record: (GenericRecord, GenericRecord), topicName: TopicName): F[Either[PublishError, Unit]] =
        Sync[F].pure(Right(()))

      override def consumeMessages(topicName: TopicName, consumerGroup: String): fs2.Stream[F, (GenericRecord, GenericRecord)] =
        fs2.Stream.empty
    }
  }

  private def getGenericRecordDeserializer[F[_]: Sync]: Deserializer[F, GenericRecord] =
    Deserializer.delegate[F, GenericRecord] {
    (topic: TopicName, data: Array[Byte]) => {
      new KafkaAvroDeserializer().deserialize(topic, data).asInstanceOf[GenericRecord]
    }
  }

  private def getGenericRecordSerializer[F[_]: Sync]: Serializer[F, GenericRecord] =
    Serializer.delegate[F, GenericRecord] {
      (topic: TopicName, data: GenericRecord) => {
        new KafkaAvroSerializer().serialize(topic, data)
      }
    }

}
