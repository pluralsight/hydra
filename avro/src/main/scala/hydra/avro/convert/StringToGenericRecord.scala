package hydra.avro.convert

import java.util.UUID

import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import cats.syntax.all._
import org.apache.avro.util.Utf8
import spray.json.{JsObject, JsValue}

import scala.util.{Failure, Success, Try}

object StringToGenericRecord {

  final case class ValidationExtraFieldsError(fields: Set[String]) extends RuntimeException(
    s"Extra fields ${fields.mkString(",")} found with Strict Validation Strategy"
  )

  final case class InvalidLogicalTypeError(expected: String, received: AnyRef) extends RuntimeException(
    s"Invalid logical type. Expected $expected but received $received"
  )

  implicit class ConvertToGenericRecord(s: String) {

    private def isUuidValid(s: String): Boolean =
      Try(UUID.fromString(s)).isSuccess

    private def checkLogicalTypes(record: GenericRecord): Try[Unit] = {
      import collection.JavaConverters._
      def checkAll(avroField: AnyRef, fieldSchema: Option[Schema]): Try[Unit] = avroField match {
        case g: GenericRecord => g.getSchema.getFields.asScala.toList
          .traverse(f => checkAll(g.get(f.name), f.schema.some)).void
        case u: Utf8 if fieldSchema.exists(f => Option(f.getLogicalType).exists(_.getName == LogicalTypes.uuid.getName)) =>
          if (isUuidValid(u.toString)) Success(()) else Failure(InvalidLogicalTypeError("UUID", u.toString))
        case _ => Success(())
      }
      val fields = record.getSchema.getFields.asScala.toList
      fields.traverse(f => checkAll(record.get(f.name), f.schema.some)).void
    }

    import collection.JavaConverters._
    private def getExtraFields(json: JsValue, schema: Schema): List[String] = json match {
      case JsObject(fields) if schema.getType == Schema.Type.RECORD =>
        val allSubFields = fields.map(kv => kv._1 -> Option(schema.getField(kv._1))).filter(_._2.isEmpty)
        def checkAllSubFields: List[String] = fields.flatMap(kv => getExtraFields(kv._2, schema.getField(kv._1).schema)).toList
        allSubFields.keys.toList ++ checkAllSubFields
      case JsObject(fields) if schema.getType == Schema.Type.UNION =>
        if (fields.size > 1) {
          val schemaFieldNames = schema.getTypes.asScala.map(_.getFullName)
          fields.keys.filter(k => schemaFieldNames.contains(k)).toList
        } else {
          List.empty
        }
      case _ => List.empty
    }

    def toGenericRecord(schema: Schema, useStrictValidation: Boolean): Try[GenericRecord] = Try {
      if (useStrictValidation) {
        import spray.json._
        val newDiff = getExtraFields(s.parseJson, schema)
        if (newDiff.nonEmpty) throw ValidationExtraFieldsError(newDiff.toSet)
      }
      val decoderFactory = new DecoderFactory
      val decoder = decoderFactory.jsonDecoder(schema, s)
      val reader = new GenericDatumReader[GenericRecord](schema)
      reader.read(null, decoder)
    }.flatTap(checkLogicalTypes)
  }

}
