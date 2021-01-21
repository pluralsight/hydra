package hydra.avro.convert

import cats.implicits._
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{JsonProperties, Schema}
import spray.json._

import scala.util.{Failure, Success, Try}

object SimpleStringToGenericRecord {

  final case class UnexpectedTypeFoundInGenericRecordConversion[A](typeExpected: Class[A], found: JsValue) extends
    Exception(s"Expected ${typeExpected.getSimpleName} but found $found")
  final case class UnexpectedDefaultTypeFoundInGenericRecordConversion(expectedType: Schema.Type, found: AnyRef) extends
    Exception(s"Expected ${expectedType.getName} but found $found.")

  implicit class SimpleStringToGenericRecordOps(str: String) {

    import StringToGenericRecord._

    import collection.JavaConverters._

    private def handleRecord(json: JsValue, schema: Schema): Try[JsObject] = {
      json match {
        case JsObject(fields) =>
          schema.getFields.asScala.toList.traverse { field =>
            fields.get(field.name).map { fjson =>
              jsonToGenericRecordJson(fjson, field.schema).map(field.name -> _)
            }.getOrElse {
              if (field.hasDefaultValue) {
                defaultToJson(field)
              } else {
                jsonToGenericRecordJson(JsNull, field.schema).map(field.name -> _)
              }
            }
          }.map(f => JsObject(f.toMap))
        case other => Failure(UnexpectedTypeFoundInGenericRecordConversion[JsObject](classOf[JsObject], other))
      }
    }


    private def handleUnion(json: JsValue, schema: Schema): Try[JsValue] = {
      json match {
        case JsNull => Success(JsNull)
        case otherJson =>
          val maybeNonNullInnerType = schema.getTypes.asScala.find(_.getType != Schema.Type.NULL)
          maybeNonNullInnerType match {
            case Some(nonNullInnerType) =>
              Success(JsObject(nonNullInnerType.getFullName -> otherJson))
            case None => Success(json)
          }
      }
    }

    private def handleArray(json: JsValue, schema: Schema): Try[JsValue] = {
      val itemsSchema = schema.getElementType
      json match {
        case JsArray(items) => items.traverse(jsonToGenericRecordJson(_, itemsSchema)).map(JsArray(_))
        case _ => Failure(UnexpectedTypeFoundInGenericRecordConversion[JsArray](classOf[JsArray], json))
      }
    }

    private def handleMap(json: JsValue, schema: Schema): Try[JsValue] = {
      val itemsSchema = schema.getValueType
      json match {
        case JsObject(items) => items.toList.traverse(kv => jsonToGenericRecordJson(kv._2, itemsSchema)
          .map(kv._1 -> _)).map(l => JsObject(l.toMap))
        case _ => Failure(UnexpectedTypeFoundInGenericRecordConversion[JsObject](classOf[JsObject], json))
      }
    }

    private def jsonToGenericRecordJson(json: JsValue, schema: Schema): Try[JsValue] = schema.getType match {
      case Schema.Type.RECORD => handleRecord(json, schema)
      case Schema.Type.UNION => handleUnion(json, schema)
      case Schema.Type.ARRAY => handleArray(json, schema)
      case Schema.Type.MAP => handleMap(json, schema)
      case _ => Success(json)
    }

    def toGenericRecordSimple(schema: Schema, useStrictValidation: Boolean = false): Try[GenericRecord] = {
      val jsonValidation = if (useStrictValidation) { checkStrictValidation(str, schema) } else Success()
      val jsonOfPayload: Try[JsValue] = jsonToGenericRecordJson(str.parseJson, schema)

      jsonValidation.flatMap(_ => jsonOfPayload.flatMap(_.compactPrint.toGenericRecordPostValidation(schema)))
    }
  }

  private def defaultToJson(field: Schema.Field): Try[(String, JsValue)] = {
    /* Matches on types from field.defaultVal() which uses the org.apache.avro.util.internal.JacksonUtils toObject function */
    def defaultToJsonLoop(defaultVal: AnyRef): Try[JsValue] = {
       defaultVal match {
         case null | JsonProperties.NULL_VALUE => Success(JsNull)
         case b: java.lang.Boolean =>
           Success(JsBoolean(b))
         case _: java.lang.Integer | _: java.lang.Float | _: java.lang.Long | _: java.lang.Double =>
           Success(JsNumber(defaultVal.toString))
         case s: java.lang.String =>
           Success(JsString(s))
         case b: Array[Byte] =>
           Success(JsString(new String(b)))
         case l: java.util.List[Object] =>
           import scala.collection.JavaConverters._
           l.asScala.toList.traverse(defaultToJsonLoop).map(_.toVector).map(JsArray(_))
         case m: java.util.LinkedHashMap[String, Object] =>
           import scala.collection.JavaConverters._
           m.asScala.toList.traverse(t => defaultToJsonLoop(t._2).map(t._1 -> _)).map(_.toMap).map(JsObject(_))
         case _ => Failure(UnexpectedDefaultTypeFoundInGenericRecordConversion(field.schema.getType, defaultVal))
       }
    }
    defaultToJsonLoop(field.defaultVal()).map(field.name -> _)
  }
}
