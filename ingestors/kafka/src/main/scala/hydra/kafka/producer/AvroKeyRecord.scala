package hydra.kafka.producer

import com.pluralsight.hydra.avro.JsonConverter
import hydra.core.transport.AckStrategy
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

final case class AvroKeyRecord(
    destination: String,
    keySchema: Schema,
    valueSchema: Schema,
    key: GenericRecord,
    payload: GenericRecord,
    ackStrategy: AckStrategy
) extends KafkaRecord[GenericRecord, GenericRecord]

object AvroKeyRecord {

  def apply(
      destination: String,
      keySchema: Schema,
      valueSchema: Schema,
      keyJson: String,
      valueJson: String,
      ackStrategy: AckStrategy
  ): AvroKeyRecord = {

    val (key, value): (GenericRecord, GenericRecord) = {
      val keyConverter: String => GenericRecord =
        new JsonConverter[GenericRecord](keySchema).convert
      val valueConverter: String => GenericRecord =
        new JsonConverter[GenericRecord](valueSchema).convert
      (keyConverter(keyJson), valueConverter(valueJson))
    }

    AvroKeyRecord(destination, keySchema, valueSchema, key, value, ackStrategy)
  }

  def apply(
      destination: String,
      keySchema: Schema,
      valueSchema: Schema,
      key: GenericRecord,
      value: GenericRecord,
      ackStrategy: AckStrategy
  ): AvroKeyRecord = {
    new AvroKeyRecord(
      destination,
      keySchema,
      valueSchema,
      key,
      value,
      ackStrategy
    )
  }
}
