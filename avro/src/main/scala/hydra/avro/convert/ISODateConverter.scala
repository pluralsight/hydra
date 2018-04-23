package hydra.avro.convert

import java.time.{OffsetDateTime, ZoneId, ZonedDateTime}

import org.apache.avro.{Conversion, LogicalType, Schema}

class ISODateConverter extends Conversion[ZonedDateTime] {

  override def getLogicalTypeName: String = IsoDate.IsoDateLogicalTypeName

  override def getConvertedType: Class[ZonedDateTime] = classOf[ZonedDateTime]

  override def fromCharSequence(value: CharSequence,
                                schema: Schema, `type`: LogicalType): ZonedDateTime = {
    val odt = OffsetDateTime.parse(value)
    odt.toLocalDateTime.atZone(ZoneId.of("UTC"))
  }
}

object IsoDate extends LogicalType("iso-date") {
  val IsoDateLogicalTypeName = "iso-date"

  override def validate(schema: Schema): Unit = {
    if (schema.getType() != Schema.Type.STRING) {
      throw new IllegalArgumentException("Iso-date can only be used with an underlying string type")
    }
  }
}
