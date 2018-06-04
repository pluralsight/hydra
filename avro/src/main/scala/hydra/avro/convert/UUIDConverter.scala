package hydra.avro.convert

import org.apache.avro.{LogicalType, Schema}


object UUID extends LogicalType("uuid") {
  override def validate(schema: Schema): Unit = {
    if (schema.getType != Schema.Type.STRING) {
      throw new IllegalArgumentException("uuid can only be used with an underlying string type")
    }
  }
}
