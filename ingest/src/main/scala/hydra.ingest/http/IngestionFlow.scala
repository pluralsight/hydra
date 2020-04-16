package hydra.ingest.http

import cats.MonadError
import cats.implicits._
import hydra.avro.registry.SchemaRegistry
import hydra.avro.util.SchemaWrapper
import hydra.core.ingest.HydraRequest
import hydra.core.ingest.RequestParams.HYDRA_KAFKA_TOPIC_PARAM
import hydra.core.transport.AckStrategy
import hydra.kafka.algebras.KafkaClientAlgebra
import hydra.kafka.producer.AvroRecord
import org.apache.avro.Schema

import scala.util.Try

final class IngestionFlow[F[_]: MonadError[*[_], Throwable]](schemaRegistry: SchemaRegistry[F], kafkaClient: KafkaClientAlgebra[F]) {
  private def getValueSchema(topicName: String): F[Schema] = {
    schemaRegistry.getSchemaBySubject(topicName + "-value")
      .flatMap(maybeSchema => MonadError[F, Throwable].fromOption(maybeSchema, new Exception))
  }

  def ingest(request: HydraRequest): F[Unit] = {
    request.metadataValue(HYDRA_KAFKA_TOPIC_PARAM) match {
      case Some(topic) => getValueSchema(topic).flatMap { valueSchema =>
        val ar = AvroRecord(topic, valueSchema, None, request.payload, AckStrategy.Replicated)
        // TODO: Support null keys, support v2
        val key = SchemaWrapper.from(valueSchema)
          .primaryKeys
          .flatMap(pkName => Try(ar.payload.get(pkName)).toOption)
          .mkString("|")
        kafkaClient.publishStringKeyMessage((key, ar.payload), topic)
      }.void
      case None => throw new Exception
    }
  }
}
