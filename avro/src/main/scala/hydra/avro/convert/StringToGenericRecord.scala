package hydra.avro.convert

import java.util.UUID

import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import cats.syntax.all._
import org.apache.avro.util.Utf8

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

    private def getAllPayloadFieldNames: Set[String] = {
      import spray.json._
      def loop(cur: JsValue, extraName: Option[String]): Set[String] = cur match {
        case JsObject(f) => f.flatMap { case (k: String, v: JsValue) =>
          loop(v, k.some) ++ Set(extraName.getOrElse("") + k)
        }.toSet
        case _ => Set.empty
      }
      loop(s.parseJson, None)
    }

    private def getAllSchemaFieldNames(schema: Schema): Set[String] = {
      import Schema.Type._
      import collection.JavaConverters._
      def loop(sch: Schema, extraName: Option[String]): Set[String] = sch.getType match {
        case RECORD => sch.getFields.asScala.toSet.flatMap { f: Schema.Field =>
          loop(f.schema, f.name.some) ++ Set(extraName.getOrElse("") + f.name)
        }
        case UNION => sch.getTypes.asScala.toSet.flatMap(loop(_, extraName))
        case other => Set(extraName.getOrElse("") + other.name.toLowerCase)
      }
      loop(schema, None)
    }

    def toGenericRecord(schema: Schema, useStrictValidation: Boolean): Try[GenericRecord] = Try {
      if (useStrictValidation) {
        val diff = getAllPayloadFieldNames diff getAllSchemaFieldNames(schema)
        if (diff.nonEmpty) throw ValidationExtraFieldsError(diff)
      }
      val decoderFactory = new DecoderFactory
      val decoder = decoderFactory.jsonDecoder(schema, s)
      val reader = new GenericDatumReader[GenericRecord](schema)
      reader.read(null, decoder)
    }.flatTap(checkLogicalTypes)
  }

}
