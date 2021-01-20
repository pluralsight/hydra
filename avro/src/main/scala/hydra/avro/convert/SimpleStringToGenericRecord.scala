package hydra.avro.convert

import org.apache.avro.{JsonProperties, Schema}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}

import scala.util.{Failure, Success, Try}
import spray.json._
import cats.implicits._
import org.apache.avro.Schema.Type._

object SimpleStringToGenericRecord {

  final case class UnexpectedTypeFoundInGenericRecordConversion[A](typeExpected: Class[A], found: JsValue) extends
    Exception(s"Expected ${typeExpected.getSimpleName} but found $found")

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

  private def defaultToJson(field: Schema.Field): Try[(String, JsValue)] = Try {
    def defaultToJsonLoop(defaultVal: AnyRef): JsValue = {
       defaultVal match {
         case null | JsonProperties.NULL_VALUE => JsNull
         case b: java.lang.Boolean =>
           JsBoolean(b)
         case _: java.lang.Integer | _: java.lang.Float | _: java.lang.Long | _: java.lang.Double =>
           JsNumber(defaultVal.toString)
         case s: java.lang.String =>
           JsString(s)
         case b: Array[Byte] =>
           JsString(new String(b))
         case l: java.util.List[Object] =>
           import scala.collection.JavaConverters._
           JsArray(l.asScala.map(defaultToJsonLoop).toVector)
         case m: java.util.LinkedHashMap[String, Object] =>
           import scala.collection.JavaConverters._
           JsObject(m.asScala.map(t => t._1 -> defaultToJsonLoop(t._2)).toMap)
       }
    }
    field.name -> defaultToJsonLoop(field.defaultVal())
  }
}
