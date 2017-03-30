package hydra.kafka.producer

import hydra.core.ingest.IngestionParams._
import hydra.core.ingest.{HydraRequest, InvalidRequestException}
import hydra.core.transport.RecordFactory
import hydra.kafka.producer.KafkaRecordFactory.KeyInterpreter


/**
  * Created by alexsilva on 3/2/17.
  */
trait KafkaRecordFactory[K, V] extends RecordFactory[K, V] {

  def getKey(key: K, value: V)(implicit ev: KeyInterpreter[K, V]): K = ev.extractKey(key, value)

  //Payload is always a string right now. We'll change it later to support more types.
  def getKey(request: HydraRequest)(implicit ev: KeyInterpreter[String, String]): Option[String] =
    request.metadataValue(HYDRA_RECORD_KEY_PARAM).map(key => ev.extractKey(key, request.payload))

  def getTopic(request: HydraRequest): String = {
    request.metadataValue(HYDRA_KAFKA_TOPIC_PARAM)
      .getOrElse(throw new InvalidRequestException("No kafka topic present in the request.", request))
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