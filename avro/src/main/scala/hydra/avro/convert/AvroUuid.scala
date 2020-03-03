package hydra.avro.convert

import org.apache.avro.{LogicalType, Schema}

object AvroUuid extends LogicalType("uuid") {
  val AvroUuidLogicalTypeName = "uuid"

  override def validate(schema: Schema): Unit = {
    if (schema.getType() != Schema.Type.STRING) {
      throw new IllegalArgumentException(
        "uui can only be used with an underlying string type"
      )
    }
  }
}
