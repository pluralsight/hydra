package hydra.avro.convert

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.util.Try
import spray.json._

object SimpleStringToGenericRecord {

  implicit class SimpleStringToGenericRecordOps(str: String) {

    import StringToGenericRecord._
    import collection.JavaConverters._

    private def jsonToGenericRecordJson(json: JsValue, schema: Schema): JsValue = schema.getType match {
      case Schema.Type.RECORD =>
        val newFieldsMap = schema.getFields.asScala.map { field =>
          val maybeThisFieldJson = json match {
            case JsObject(fields) => fields.get(field.name)
            case other => ??? // TODO do not merge
          }
          maybeThisFieldJson match {
            case Some(fjson) => field.name -> jsonToGenericRecordJson(fjson, field.schema)
            case None => field.name -> jsonToGenericRecordJson(JsNull, field.schema)
          }
        }.toMap
        JsObject(newFieldsMap)
      case Schema.Type.UNION =>
        json match {
          case JsNull => JsNull
          case otherJson =>
            val maybeNonNullInnerType = schema.getTypes.asScala.find(_.getType != Schema.Type.NULL)
            maybeNonNullInnerType match {
              case Some(nonNullInnerType) =>
                JsObject(nonNullInnerType.getFullName -> otherJson)
              case None => ??? // TODO return error DO NOT MERGE
            }
        }
      case other => json
    }

    def toGenericRecordSimple(schema: Schema, useStrictValidation: Boolean): Try[GenericRecord] = {
      val jsonOfPayload = jsonToGenericRecordJson(str.parseJson, schema)
      jsonOfPayload.compactPrint.toGenericRecord(schema, useStrictValidation)
    }

  }

}
