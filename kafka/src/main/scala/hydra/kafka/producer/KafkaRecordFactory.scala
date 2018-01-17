package hydra.kafka.producer

import hydra.core.ingest.{HydraRequest, InvalidRequestException, RequestParams}
import hydra.core.ingest.RequestParams._
import hydra.core.transport.RecordFactory
import hydra.kafka.producer.KafkaRecordFactory.KeyInterpreter

import scala.util.{Failure, Success, Try}


/**
  * Created by alexsilva on 3/2/17.
  */
trait KafkaRecordFactory[K, V] extends RecordFactory[K, V] {

  def getKey(key: K, value: V)(implicit ev: KeyInterpreter[K, V]): K = ev.extractKey(key, value)

  //Payload is always a string right now. We'll change it later to support more types.
  def getKey(request: HydraRequest)(implicit ev: KeyInterpreter[String, String]): Option[String] =
    request.metadataValue(HYDRA_RECORD_KEY_PARAM).map(key => ev.extractKey(key, request.payload))

  def getTopic(request: HydraRequest): Try[String] = {
    request.metadataValue(HYDRA_KAFKA_TOPIC_PARAM).map(Success(_))
      .getOrElse(Failure(InvalidRequestException("No kafka topic present in the request.",
        request)))
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
      case None => Failure(InvalidRequestException("No kafka topic present in the request.",
        request))
    }
  }
}


object KafkaRecordFactory {

  trait KeyInterpreter[K, V] {

    def extractKey(key: K, payload: V): K
  }

  object KeyInterpreter {

    implicit object JsonKeyInterpreter extends KeyInterpreter[String, String] {
      override def extractKey(key: String, payload: String): String = JsonPathKeys.getKey(key, payload)
    }

  }

}