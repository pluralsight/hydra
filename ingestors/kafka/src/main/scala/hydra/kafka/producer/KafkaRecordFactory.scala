package hydra.kafka.producer

import com.fasterxml.jackson.databind.JsonNode
import hydra.avro.util.SchemaWrapper
import hydra.core.ingest.RequestParams._
import hydra.core.ingest.{HydraRequest, RequestParams}
import hydra.core.protocol.MissingMetadataException
import hydra.core.transport.RecordFactory
import hydra.kafka.producer.KafkaRecordFactory.RecordKeyExtractor
import org.apache.avro.generic.GenericRecord

import scala.util.{Failure, Success, Try}

/**
  * Created by alexsilva on 3/2/17.
  */
trait KafkaRecordFactory[K, V] extends RecordFactory[K, V] {

  def getKey(request: HydraRequest, value: V)(
      implicit ev: RecordKeyExtractor[K, V]
  ): Option[K] =
    ev.extractKeyValue(request, value)

  //delete requests
  def getKey(request: HydraRequest): Option[String] =
    request.metadataValue(HYDRA_RECORD_KEY_PARAM)

  def getTopic(request: HydraRequest): Try[String] = {
    request
      .metadataValue(HYDRA_KAFKA_TOPIC_PARAM)
      .map(Success(_))
      .getOrElse(
        Failure(
          MissingMetadataException(
            HYDRA_KAFKA_TOPIC_PARAM,
            "No kafka topic present in the request."
          )
        )
      )
  }

  /**
    * Topic is always required and is defined under the `HYDRA_KAFKA_TOPIC_PARAM` header.
    * This will be a Failure if no topic is provided.
    *
    * Schema subject is an optional parameter used for looking up the schema.
    * The no schema subject is provided in the request, the topic name is used as the subject.
    *
    * @param request
    * @return A tuple where the first element is the topic and the second is the subject.
    *         If no subject is supplied, both first and second elements will be the topic name.
    */
  def getTopicAndSchemaSubject(request: HydraRequest): Try[(String, String)] = {
    val subject = request.metadataValue(RequestParams.HYDRA_SCHEMA_PARAM)
    request.metadataValue(HYDRA_KAFKA_TOPIC_PARAM) match {
      case Some(topic) => Success(topic -> subject.getOrElse(topic))
      case None =>
        Failure(
          MissingMetadataException(
            HYDRA_KAFKA_TOPIC_PARAM,
            "No kafka topic present in the request."
          )
        )
    }
  }
}

object KafkaRecordFactory {

  trait RecordKeyExtractor[K, V] {

    def extractKeyValue(request: HydraRequest, record: V): Option[K]
  }

  object RecordKeyExtractor {

    implicit object StringRecordKeyExtractor
        extends RecordKeyExtractor[String, String] {

      override def extractKeyValue(
          request: HydraRequest,
          record: String
      ): Option[String] = {
        request
          .metadataValue(HYDRA_RECORD_KEY_PARAM)
          .map(key => JsonPathKeys.getKey(key, record))
      }
    }

    implicit object JsonRecordKeyExtractor
        extends RecordKeyExtractor[String, JsonNode] {

      override def extractKeyValue(
          request: HydraRequest,
          record: JsonNode
      ): Option[String] = {
        request
          .metadataValue(HYDRA_RECORD_KEY_PARAM)
          .map(key => JsonPathKeys.getKey(key, record.toString))
      }
    }

    implicit object SchemaKeyExtractor
        extends RecordKeyExtractor[String, GenericRecord] {

      override def extractKeyValue(
          request: HydraRequest,
          payload: GenericRecord
      ): Option[String] = {
        request
          .metadataValue(HYDRA_RECORD_KEY_PARAM)
          .map { key => JsonPathKeys.getKey(key, request.payload) }
          .orElse {
            val schema = payload.getSchema
            val wrapper = SchemaWrapper.from(schema)
            wrapper
              .validate()
              .get //we're throwing the exception here so that the request ends with a 400
            wrapper.primaryKeys.map(payload.get) match {
              case Nil  => None
              case keys => Some(keys.mkString("|"))
            }
          }
      }
    }

  }

}
