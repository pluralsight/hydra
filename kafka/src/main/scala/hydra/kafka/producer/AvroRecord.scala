package hydra.kafka.producer

import com.pluralsight.hydra.avro.JsonConverter
import hydra.core.transport.DeliveryStrategy
import hydra.core.transport.DeliveryStrategy.BestEffort
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

/**
  * Created by alexsilva on 10/30/15.
  */
case class AvroRecord(destination: String, schema: Schema, key: Option[String], json: String,
                      deliveryStrategy: DeliveryStrategy = BestEffort) extends KafkaRecord[String, GenericRecord] {

  val payload: GenericRecord = {
    val converter: JsonConverter[GenericRecord] = new JsonConverter[GenericRecord](schema)
    converter.convert(json)
  }
}
