package hydra.ingest.services

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

  import IngestionFlow._

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
        val payloadMaybe = Option(ar.payload)
        // TODO: Support v2
        val key = schemaWrapper.primaryKeys.toList match {
          case Nil => None
          case l => l.flatMap(pkName => payloadMaybe.flatMap(p => Try(p.get(pkName)).toOption))
            .mkString("|").some
        }
        kafkaClient.publishStringKeyMessage((key, payloadMaybe), topic)
      }.void
      case None => MonadError[F, Throwable].raiseError(MissingTopicNameException(request))
    }
  }
}

object IngestionFlow {
  final case class MissingTopicNameException(request: HydraRequest)
    extends Exception(s"Missing the topic name in request with correlationId ${request.correlationId}")
}
