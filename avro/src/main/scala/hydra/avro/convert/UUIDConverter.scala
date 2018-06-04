package hydra.avro.convert

import java.util.UUID

import org.apache.avro
import org.apache.avro.{Conversion, LogicalType, Schema}

class UUIDConverter extends Conversion[UUID] {

  override def getConvertedType: Class[UUID] = classOf[UUID]

  override def getLogicalTypeName: String = HydraUUID.getName

  override def fromCharSequence(value: CharSequence,
                                schema: avro.Schema, `type`: LogicalType): UUID = {
    UUID.fromString(value.toString)
  }
}

object HydraUUID extends LogicalType("hydra-uuid") {
  override def validate(schema: Schema): Unit = {
    if (schema.getType != Schema.Type.STRING) {
      throw new IllegalArgumentException("hydra-uuid can only be used with an underlying string type")
    }
  }
}
