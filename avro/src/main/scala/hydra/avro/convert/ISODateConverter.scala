package hydra.avro.convert

import java.text.SimpleDateFormat
import java.time._

import hydra.common.logging.LoggingAdapter
import org.apache.avro.{Conversion, LogicalType, Schema}

import scala.util.Try

/**
  * Converts strings in ISO format to a ZonedDateTime in the UTC time zone.
  *
  * If the string is not formatted properly, the EPOCH is returned.
  */
class ISODateConverter extends Conversion[ZonedDateTime] with LoggingAdapter {

  private val utc = ZoneOffset.UTC

  override def getLogicalTypeName: String = IsoDate.IsoDateLogicalTypeName

  override def getConvertedType: Class[ZonedDateTime] = classOf[ZonedDateTime]

  private val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")

  override def fromCharSequence(
      value: CharSequence,
      schema: Schema,
      `type`: LogicalType
  ): ZonedDateTime = {
    Try(OffsetDateTime.parse(value).toInstant)
      .orElse {
        Try(LocalDateTime.parse(value).toInstant(ZoneOffset.UTC))
      }
      .orElse {
        Try(simpleDateFormat.parse(value.toString).toInstant)
      }
      .recover {
        case e: Throwable =>
          log.error(e.getMessage, e)
          Instant.EPOCH
      }
      .map(_.atZone(utc))
      .get
  }
}

object IsoDate extends LogicalType("iso-datetime") {
  val IsoDateLogicalTypeName = "iso-datetime"

  override def validate(schema: Schema): Unit = {
    if (schema.getType() != Schema.Type.STRING) {
      throw new IllegalArgumentException(
        "Iso-datetime can only be used with an underlying string type"
      )
    }
  }
}
