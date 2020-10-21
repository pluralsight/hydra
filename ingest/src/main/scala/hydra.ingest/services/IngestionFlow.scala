package hydra.ingest.services

import java.io.IOException

import cats.MonadError
import cats.syntax.all._
import com.pluralsight.hydra.avro.JsonToAvroConversionException
import hydra.avro.registry.SchemaRegistry
import hydra.avro.resource.SchemaResourceLoader.SchemaNotFoundException
import hydra.avro.util.SchemaWrapper
import hydra.core.ingest.HydraRequest
import hydra.core.ingest.RequestParams.{HYDRA_KAFKA_TOPIC_PARAM, HYDRA_RECORD_KEY_PARAM}
import hydra.core.transport.{AckStrategy, ValidationStrategy}
import hydra.kafka.algebras.KafkaClientAlgebra
import hydra.kafka.producer.AvroRecord
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import scalacache._
import scalacache.guava._
import scalacache.memoization._

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

final class IngestionFlow[F[_]: MonadError[*[_], Throwable]: Mode](
                                                                    schemaRegistry: SchemaRegistry[F],
                                                                    kafkaClient: KafkaClientAlgebra[F],
                                                                    schemaRegistryBaseUrl: String
                                                                  ) {

  import IngestionFlow._

  implicit val guavaCache: Cache[SchemaWrapper] = GuavaCache[SchemaWrapper]

  private def getValueSchema(topicName: String): F[Schema] = {
    schemaRegistry.getLatestSchemaBySubject(topicName + "-value")
      .flatMap { maybeSchema =>
        val schemaNotFound = SchemaNotFoundException(topicName)
        MonadError[F, Throwable].fromOption(maybeSchema, SchemaNotFoundAugmentedException(schemaNotFound, topicName))
      }
  }

  private def getValueSchemaWrapper(topicName: String): F[SchemaWrapper] = memoizeF[F, SchemaWrapper](Some(2.minutes)) {
    getValueSchema(topicName).map { valueSchema =>
      SchemaWrapper.from(valueSchema)
    }
  }

  def ingest(request: HydraRequest): F[Unit] = {
    request.metadataValue(HYDRA_KAFKA_TOPIC_PARAM) match {
      case Some(topic) => getValueSchemaWrapper(topic).flatMap { schemaWrapper =>
        val useStrictValidation = request.validationStrategy == ValidationStrategy.Strict
        val payloadTryMaybe: Try[Option[GenericRecord]] = Option(request.payload) match {
          case Some(p) => convertToAvro(topic, schemaWrapper, useStrictValidation, p).map(avroRecord => Some(avroRecord.payload))
          case None => Success(None)
        }
        val v1Key = getV1RecordKey(schemaWrapper, payloadTryMaybe, request)
        MonadError[F, Throwable].fromTry(payloadTryMaybe).flatMap { payloadMaybe =>
          kafkaClient.publishStringKeyMessage((v1Key, payloadMaybe, None), topic).void
        }
      }
      case None => MonadError[F, Throwable].raiseError(MissingTopicNameException(request))
    }
  }

  private def getV1RecordKey(schemaWrapper: SchemaWrapper, payloadTryMaybe: Try[Option[GenericRecord]], request: HydraRequest): Option[String] = {
    val headerV1Key = request.metadata.get(HYDRA_RECORD_KEY_PARAM)
    val optionString = schemaWrapper.primaryKeys.toList match {
      case Nil => None
      case l => l.flatMap(pkName => payloadTryMaybe match {
        case Success(payloadMaybe) =>
          payloadMaybe.flatMap(p => Try(p.get(pkName)).toOption)
        case Failure(_) => None
      }).mkString("|").some
    }
    headerV1Key.orElse(optionString)
  }

  private def convertToAvro(topic: String, schemaWrapper: SchemaWrapper, useStrictValidation: Boolean, payloadString: String): Try[AvroRecord] = {
    Try(AvroRecord(topic, schemaWrapper.schema, None, payloadString, AckStrategy.Replicated, useStrictValidation)).recoverWith {
      case e: JsonToAvroConversionException =>
        val location = s"$schemaRegistryBaseUrl/subjects/$topic-value/versions/latest/schema"
        Failure(new AvroConversionAugmentedException(s"${e.getClass.getName}: ${e.getMessage} [$location]"))
      case e: IOException =>
        val location = s"$schemaRegistryBaseUrl/subjects/$topic-value/versions/latest/schema"
        Failure(new AvroConversionAugmentedException(s"${e.getMessage} [$location]"))
      case e => Failure(e)
    }
  }
}

object IngestionFlow {
  final case class MissingTopicNameException(request: HydraRequest)
    extends Exception(s"Missing the topic name in request with correlationId ${request.correlationId}")
  final case class AvroConversionAugmentedException(message: String) extends RuntimeException(message)
  final case class SchemaNotFoundAugmentedException(schemaNotFoundException: SchemaNotFoundException, topic: String)
    extends RuntimeException(s"Schema '$topic' cannot be loaded. Cause: ${schemaNotFoundException.getClass.getName}: Schema not found for $topic")
}
