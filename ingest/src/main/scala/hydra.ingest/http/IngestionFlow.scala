package hydra.ingest.http

import cats.effect.Sync
import hydra.avro.registry.SchemaRegistry
import hydra.core.ingest.HydraRequest
import hydra.kafka.algebras.KafkaClientAlgebra
import cats.implicits._
import hydra.core.ingest.RequestParams.HYDRA_KAFKA_TOPIC_PARAM
import hydra.core.transport.AckStrategy
import hydra.kafka.producer.AvroRecord
import org.apache.avro.Schema

trait IngestionFlow[F[_]] {
  def ingest(request: HydraRequest): F[Unit]
}

object IngestionFlow {

  def live[F[_]: Sync](schemaRegistry: SchemaRegistry[F], kafkaClient: KafkaClientAlgebra[F]): F[IngestionFlow[F]] = Sync[F].delay {
    new IngestionFlow[F] {

      private def getValueSchema(topicName: String): F[Schema] = {
        schemaRegistry.getSchemaBySubject(topicName + "-value")
          .flatMap(maybeSchema => Sync[F].fromOption(maybeSchema, new Exception))
      }

      override def ingest(request: HydraRequest): F[Unit] = {
        request.metadataValue(HYDRA_KAFKA_TOPIC_PARAM) match {
          case Some(topic) => getValueSchema(topic).flatMap { valueSchema =>
            val ar = AvroRecord(topic, valueSchema, None, request.payload, AckStrategy.Replicated)
            kafkaClient.publishStringKeyMessage(("asdf", ar.payload), topic)
          }
          case None => throw Exception
        }
      }
    }
  }

}
