package hydra.ingest.services

import java.io.IOException

import cats.MonadError
import cats.syntax.all._
import fs2.kafka.{Header, Headers}
import hydra.avro.registry.SchemaRegistry
import hydra.avro.resource.SchemaResourceLoader.SchemaNotFoundException
import hydra.avro.util.SchemaWrapper
import hydra.core.transport.ValidationStrategy
import hydra.kafka.algebras.KafkaClientAlgebra
import hydra.kafka.algebras.KafkaClientAlgebra.PublishResponse
import hydra.kafka.model.TopicMetadataV2Request.Subject
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import scalacache._
import scalacache.guava._
import scalacache.memoization._

import scala.concurrent.duration._
import scala.util.{Failure, Try}

final class IngestionFlowV2[F[_]: MonadError[*[_], Throwable]: Mode](
                                                                    schemaRegistry: SchemaRegistry[F],
                                                                    kafkaClient: KafkaClientAlgebra[F],
                                                                    schemaRegistryBaseUrl: String) {

  import IngestionFlowV2._
  import hydra.avro.convert.StringToGenericRecord._
  import hydra.avro.convert.SimpleStringToGenericRecord._

  implicit val guavaCache: Cache[SchemaWrapper] = GuavaCache[SchemaWrapper]

  private def getSchema(subject: String): F[Schema] = {
    schemaRegistry.getLatestSchemaBySubject(subject)
      .flatMap { maybeSchema =>
        val schemaNotFound = SchemaNotFoundException(subject)
        MonadError[F, Throwable].fromOption(maybeSchema, SchemaNotFoundAugmentedException(schemaNotFound, subject))
      }
  }

  private def getSchemaWrapper(subject: Subject, isKey: Boolean): F[SchemaWrapper] = memoizeF[F, SchemaWrapper](Some(2.minutes)) {
    val suffix = if (isKey) "-key" else "-value"
    getSchema(subject.value + suffix).map { sch =>
      SchemaWrapper.from(sch)
    }
  }

  private def recover[A](subject: Subject, isKey: Boolean): PartialFunction[Throwable, Try[A]] = {
    val suffix = if (isKey) "-key" else "-value"
    val location = s"$schemaRegistryBaseUrl/subjects/${subject.value}$suffix/versions/latest/schema"
    val pf: PartialFunction[Throwable, Try[A]] = {
      case e: ValidationExtraFieldsError =>
        Failure(AvroConversionAugmentedException(s"${e.getClass.getName}: ${e.getMessage} [$location]"))
      case e: InvalidLogicalTypeError =>
        Failure(AvroConversionAugmentedException(s"${e.getClass.getName}: ${e.getMessage} [$location]"))
      case e: IOException =>
        Failure(AvroConversionAugmentedException(s"${e.getClass.getName}: ${e.getMessage} [$location]"))
      case e => Failure(e)
    }
    pf
  }

  private def getSchemas(request: V2IngestRequest, topic: Subject): F[(GenericRecord, Option[GenericRecord])] = {
    val useStrictValidation = request.validationStrategy.getOrElse(ValidationStrategy.Strict) == ValidationStrategy.Strict
    def getRecord(payload: String, schema: Schema): Try[GenericRecord] = if (request.useSimpleJsonFormat) {
      payload.toGenericRecordSimple(schema, useStrictValidation)
    } else {
      payload.toGenericRecord(schema, useStrictValidation)
    }

    for {
      kSchema <- getSchemaWrapper(topic, isKey = true)
      vSchema <- getSchemaWrapper(topic, isKey = false)
      k <- MonadError[F, Throwable].fromTry(
        getRecord(request.keyPayload, kSchema.schema).recoverWith(recover(topic, isKey = true)))
      v <- MonadError[F, Throwable].fromTry(
        request.valPayload.traverse(getRecord(_, vSchema.schema)).recoverWith(recover(topic, isKey = false)))
    } yield (k, v)
  }

  def ingest(request: V2IngestRequest, topic: Subject): F[PublishResponse] = {
    getSchemas(request, topic).flatMap { case (key, value) =>
      kafkaClient.publishMessage((key, value, request.headers), topic.value).rethrow
    }
  }
}

object IngestionFlowV2 {
  final case class V2IngestRequest(keyPayload: String, valPayload: Option[String],
                                   validationStrategy: Option[ValidationStrategy],
                                   useSimpleJsonFormat: Boolean,
                                   headers: Option[Headers] = None)

  final case class AvroConversionAugmentedException(message: String) extends RuntimeException(message)
  final case class SchemaNotFoundAugmentedException(schemaNotFoundException: SchemaNotFoundException, topic: String)
    extends RuntimeException(s"Schema '$topic' cannot be loaded. Cause: ${schemaNotFoundException.getClass.getName}: Schema not found for $topic")
}
