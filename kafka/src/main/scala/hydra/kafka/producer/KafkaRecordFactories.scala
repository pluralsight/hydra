package hydra.kafka.producer

import hydra.core.ingest.HydraRequest
import hydra.core.ingest.RequestParams.HYDRA_RECORD_FORMAT_PARAM
import hydra.core.protocol.{InvalidRequest, MessageValidationResult}

/**
  * A type class wrapper around the RecordFactory implementations for Kafka.
  *
  * Created by alexsilva on 2/23/17.
  */
object KafkaRecordFactories {

  def validate(request: HydraRequest): MessageValidationResult = {
    request.metadataValue(HYDRA_RECORD_FORMAT_PARAM) match {
      case Some(value) if (value.equalsIgnoreCase("string")) => StringRecordFactory.validate(request)
      case Some(value) if (value.equalsIgnoreCase("json")) => JsonRecordFactory.validate(request)
      case Some(value) if (value.equalsIgnoreCase("avro")) => AvroRecordFactory.validate(request)
      case Some(value) => InvalidRequest(new IllegalArgumentException(s"'$value' is not a valid format."))
      case _ => AvroRecordFactory.validate(request)
    }
  }

  def build(request: HydraRequest): KafkaRecord[_, _] = {
    request.metadataValue(HYDRA_RECORD_FORMAT_PARAM) match {
      case Some(value) if (value.equalsIgnoreCase("string")) => StringRecordFactory.build(request)
      case Some(value) if (value.equalsIgnoreCase("json")) => JsonRecordFactory.build(request)
      case Some(value) if (value.equalsIgnoreCase("avro")) => AvroRecordFactory.build(request)
      case Some(value) => throw new IllegalArgumentException(s"'$value' is not a valid format.")
      case _ => AvroRecordFactory.build(request)
    }
  }
}
