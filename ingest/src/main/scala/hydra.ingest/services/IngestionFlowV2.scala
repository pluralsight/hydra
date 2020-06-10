package hydra.ingest.services

import java.io.IOException

import cats.MonadError
import cats.implicits._
import com.pluralsight.hydra.avro.JsonToAvroConversionException
import hydra.avro.registry.SchemaRegistry
import hydra.avro.resource.SchemaResourceLoader.SchemaNotFoundException
import hydra.avro.util.SchemaWrapper
import hydra.core.transport.ValidationStrategy
import hydra.kafka.algebras.KafkaClientAlgebra
import hydra.kafka.model.TopicMetadataV2Request.Subject
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
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
      case e: JsonToAvroConversionException =>
        Failure(AvroConversionAugmentedException(s"${e.getClass.getName}: ${e.getMessage} [$location]"))
      case e: ValidationExtraFieldsError =>
        Failure(AvroConversionAugmentedException(s"${e.getClass.getName}: ${e.getMessage} [$location]"))
      case e: IOException =>
        Failure(AvroConversionAugmentedException(s"${e.getClass.getName}: ${e.getMessage} [$location]"))
      case e => Failure(e)
    }
    pf
  }

  private def getSchemas(request: V2IngestRequest): F[(GenericRecord, Option[GenericRecord])] = {
    val useStrictValidation = request.validationStrategy == ValidationStrategy.Strict
    def getRecord(payload: String, schema: Schema): Try[GenericRecord] =
      payload.toGenericRecord(schema, useStrictValidation)
    for {
      kSchema <- getSchemaWrapper(request.topic, isKey = true)
      vSchema <- getSchemaWrapper(request.topic, isKey = false)
      k <- MonadError[F, Throwable].fromTry(
        getRecord(request.keyPayload, kSchema.schema).recoverWith(recover(request.topic, isKey = true)))
      v <- MonadError[F, Throwable].fromTry(
        request.valPayload.traverse(getRecord(_, vSchema.schema)).recoverWith(recover(request.topic, isKey = false)))
    } yield (k, v)
  }

  def ingest(request: V2IngestRequest): F[Unit] = {
    getSchemas(request).flatMap { case (key, value) =>
      kafkaClient.publishMessage((key, value), request.topic.value).void
    }
  }
}

object IngestionFlowV2 {

  private implicit class ConvertToGenericRecord(s: String) {

    private def getAllPayloadFieldNames: Set[String] = {
      import spray.json._
      def loop(cur: JsValue): Set[String] = cur match {
        case JsObject(f) => f.keySet ++ f.values.toSet.flatMap(loop)
        case _ => Set.empty
      }
      loop(s.parseJson)
    }

    private def getAllSchemaFieldNames(schema: Schema): Set[String] = {
      import Schema.Type._

      import collection.JavaConverters._
      def loop(sch: Schema): Set[String] = sch.getType match {
        case RECORD => schema.getFields.asScala.toSet.flatMap { f: Schema.Field =>
          loop(f.schema) ++ Set(f.name)
        }
        case _ => Set.empty
      }
      loop(schema)
    }

    def toGenericRecord(schema: Schema, useStrictValidation: Boolean): Try[GenericRecord] = Try {
      if (useStrictValidation) {
        val diff = getAllPayloadFieldNames diff getAllSchemaFieldNames(schema)
        if (diff.nonEmpty) throw ValidationExtraFieldsError(diff)
      }
      val decoderFactory = new DecoderFactory
      val decoder = decoderFactory.jsonDecoder(schema, s)
      val reader = new GenericDatumReader[GenericRecord](schema)
      reader.read(null, decoder)
    }
  }

  final case class V2IngestRequest(topic: Subject, keyPayload: String, valPayload: Option[String], validationStrategy: ValidationStrategy)

  final case class ValidationExtraFieldsError(fields: Set[String]) extends RuntimeException(
    s"Extra fields ${fields.mkString(",")} found with Strict Validation Strategy"
  )
  final case class AvroConversionAugmentedException(message: String) extends RuntimeException(message)
  final case class SchemaNotFoundAugmentedException(schemaNotFoundException: SchemaNotFoundException, topic: String)
    extends RuntimeException(s"Schema '$topic' cannot be loaded. Cause: ${schemaNotFoundException.getClass.getName}: Schema not found for $topic")
}
