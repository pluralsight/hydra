package hydra.kafka.producer

import com.pluralsight.hydra.avro.JsonConverter
import hydra.core.transport.{AckStrategy, DeliveryStrategy}
import hydra.core.transport.DeliveryStrategy.AtMostOnce
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

/**
  * Created by alexsilva on 10/30/15.
  */
case class AvroRecord(destination: String, schema: Schema, key: Option[String], payload: GenericRecord,
                      deliveryStrategy: DeliveryStrategy,
                      ackStrategy: AckStrategy) extends KafkaRecord[String, GenericRecord]

object AvroRecord {
  def apply(destination: String, schema: Schema, key: Option[String], json: String,
            deliveryStrategy: DeliveryStrategy = AtMostOnce,
            ackStrategy: AckStrategy = AckStrategy.None): AvroRecord = {

    val payload: GenericRecord = {
      val converter: JsonConverter[GenericRecord] = new JsonConverter[GenericRecord](schema)
      converter.convert(json)
    }

    AvroRecord(destination, schema, key, payload, deliveryStrategy, ackStrategy)
  }
}

