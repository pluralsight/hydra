package hydra.kafka.producer

import hydra.core.ingest.HydraRequest
import hydra.core.ingest.IngestionParams.HYDRA_RECORD_KEY_PARAM
import hydra.core.producer.RecordFactory
import hydra.kafka.producer.KafkaRecordFactory.KeyInterpreter


/**
  * Created by alexsilva on 3/2/17.
  */
trait KafkaRecordFactory[K, V] extends RecordFactory[K, V] {

  def getKey(key: K, value: V)(implicit ev: KeyInterpreter[K, V]): K = ev.extractKey(key, value)

  //Payload is always a string right now. We'll change it later to support more types.
  def getKey(request: HydraRequest)(implicit ev: KeyInterpreter[String, String]): Option[String] =
    request.metadataValue(HYDRA_RECORD_KEY_PARAM).map(key => ev.extractKey(key, request.payload))
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