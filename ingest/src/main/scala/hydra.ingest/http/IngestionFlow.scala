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
import scalacache._
import scalacache.guava._
import scalacache.memoization._

import scala.concurrent.duration._
import scala.util.Try

final class IngestionFlow[F[_]: MonadError[*[_], Throwable]: Mode](schemaRegistry: SchemaRegistry[F], kafkaClient: KafkaClientAlgebra[F]) {

  implicit val guavaCache: Cache[SchemaWrapper] = GuavaCache[SchemaWrapper]

  private def getValueSchema(topicName: String): F[Schema] = {
    schemaRegistry.getSchemaBySubject(topicName + "-value")
      .flatMap(maybeSchema => MonadError[F, Throwable].fromOption(maybeSchema, new Exception))
  }

  private def getValueSchemaWrapper(topicName: String): F[SchemaWrapper] = memoizeF[F, SchemaWrapper](Some(2.minutes)) {
    getValueSchema(topicName).map { valueSchema =>
      SchemaWrapper.from(valueSchema)
    }
  }

  def ingest(request: HydraRequest): F[Unit] = {
    request.metadataValue(HYDRA_KAFKA_TOPIC_PARAM) match {
      case Some(topic) => getValueSchemaWrapper(topic).flatMap { schemaWrapper =>
        val ar = AvroRecord(topic, schemaWrapper.schema, None, request.payload, AckStrategy.Replicated)
        // TODO: Support null keys, support v2
        val key = schemaWrapper
          .primaryKeys
          .flatMap(pkName => Try(ar.payload.get(pkName)).toOption)
          .mkString("|")
        kafkaClient.publishStringKeyMessage((key, ar.payload), topic)
      }.void
      case None => throw new Exception
    }
  }
}
