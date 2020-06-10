package hydra.avro.convert

import org.apache.avro.Schema
import org.apache.avro.Schema.Type.RECORD
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory

import scala.util.Try

object StringToGenericRecord {

  final case class ValidationExtraFieldsError(fields: Set[String]) extends RuntimeException(
    s"Extra fields ${fields.mkString(",")} found with Strict Validation Strategy"
  )

  implicit class ConvertToGenericRecord(s: String) {

    private def getAllPayloadFieldNames: Set[String] = {
      import spray.json._
      def loop(cur: JsValue): Set[String] = cur match {
        case JsObject(f) => f.keySet ++ f.values.toSet.flatMap(loop)
        case _ => Set.empty
      }
      loop(s.parseJson)
    }

    private def getAllSchemaFieldNames(schema: Schema): Set[String] = {
      import Schema.Type._

      import collection.JavaConverters._
      def loop(sch: Schema): Set[String] = sch.getType match {
        case RECORD => schema.getFields.asScala.toSet.flatMap { f: Schema.Field =>
          loop(f.schema) ++ Set(f.name)
        }
        case _ => Set.empty
      }
      loop(schema)
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
    }
  }

}
