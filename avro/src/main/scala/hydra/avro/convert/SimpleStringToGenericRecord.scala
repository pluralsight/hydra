package hydra.avro.convert

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.util.Try
import spray.json._

import scala.annotation.tailrec

object SimpleStringToGenericRecord {

  implicit class SimpleStringToGenericRecordOps(str: String) {

    import StringToGenericRecord._
    import collection.JavaConverters._

    private def handleRecord(json: JsValue, schema: Schema): JsObject = {
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
    }

    private def handleUnion(json: JsValue, schema: Schema): JsValue = {
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
    }

    private def handleArray(json: JsValue, schema: Schema): JsValue = {
      val itemsSchema = schema.getElementType
      json match {
        case JsArray(items) => JsArray(items.map(jsonToGenericRecordJson(_, itemsSchema)))
        case _ => ??? // TODO return error DO NOT MERGE
      }
    }

    private def jsonToGenericRecordJson(json: JsValue, schema: Schema): JsValue = schema.getType match {
      case Schema.Type.RECORD => handleRecord(json, schema)
      case Schema.Type.UNION => handleUnion(json, schema)
      case Schema.Type.ARRAY => handleArray(json, schema)
      case _ => json
    }

    def toGenericRecordSimple(schema: Schema, useStrictValidation: Boolean): Try[GenericRecord] = {
      val jsonOfPayload = jsonToGenericRecordJson(str.parseJson, schema)
      jsonOfPayload.compactPrint.toGenericRecord(schema, useStrictValidation)
    }

  }

}
