package hydra.avro.convert

import java.util.UUID

import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import cats.syntax.all._
import org.apache.avro.Schema.Type.RECORD
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

    private def isStrictCompat(inputSchema: Schema): List[String] = {
      def loop(sch: Schema, payload: JsValue): List[String] = {
        import Schema.Type._
        (sch.getType, payload) match {
          case (RECORD, JsObject(fields)) => fields.toList.flatMap { case (k, v) =>
            Try(sch.getField(k).schema).toOption match {
              case Some(subfield) => loop(subfield, v)
              case None => List(k)
            }
          }
          case _ => List.empty
        }
      }

      import spray.json._
      loop(inputSchema, s.parseJson)
    }

    def toGenericRecord(schema: Schema, useStrictValidation: Boolean): Try[GenericRecord] = Try {
      if (useStrictValidation) {
        val diff = isStrictCompat(schema)
        if (diff.nonEmpty) throw ValidationExtraFieldsError(diff.toSet)
      }
      val decoderFactory = new DecoderFactory
      val decoder = decoderFactory.jsonDecoder(schema, s)
      val reader = new GenericDatumReader[GenericRecord](schema)
      reader.read(null, decoder)
    }.flatTap(checkLogicalTypes)
  }

}
