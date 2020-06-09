package hydra.kafka.producer

import com.pluralsight.hydra.avro.JsonConverter
import hydra.core.transport.AckStrategy
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.commons.lang3.StringUtils

/**
  * Created by alexsilva on 10/30/15.
  */
case class AvroRecord(
    destination: String,
    schema: Schema,
    key: String,
    payload: GenericRecord,
    ackStrategy: AckStrategy
) extends KafkaRecord[String, GenericRecord]

object AvroRecord {

  def apply(
      destination: String,
      schema: Schema,
      key: Option[String],
      json: String,
      ackStrategy: AckStrategy,
      useStrictValidation: Boolean = false
  ): AvroRecord = {

    val payload: GenericRecord = {
      val converter: JsonConverter[GenericRecord] =
        new JsonConverter[GenericRecord](schema, useStrictValidation)
      converter.convert(json)
    }

    AvroRecord(destination, schema, key.orNull, payload, ackStrategy)
  }

  def apply(
      destination: String,
      schema: Schema,
      key: Option[String],
      record: GenericRecord,
      ackStrategy: AckStrategy
  ): AvroRecord = {
    AvroRecord(destination, schema, key.orNull, record, ackStrategy)
  }
}
