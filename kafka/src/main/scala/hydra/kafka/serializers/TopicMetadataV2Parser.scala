package hydra.kafka.serializers

import java.time.Instant

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyChain, Validated, ValidatedNec}
import cats.implicits._
import hydra.avro.resource.HydraSubjectValidator
import hydra.core.marshallers.{CurrentState, GenericServiceResponse, History, Notification, StreamType, Telemetry}
import hydra.kafka.model.{ContactMethod, Email, Schemas, Slack, TopicMetadataV2Request}
import org.apache.avro.Schema
import spray.json.{DefaultJsonProtocol, DeserializationException, JsArray, JsBoolean, JsObject, JsString, JsValue, RootJsonFormat}

import scala.util.{Failure, Success, Try}

trait TopicMetadataV2Parser extends SprayJsonSupport with DefaultJsonProtocol with TopicMetadataV2Validator with Errors {
  implicit object InstantFormat extends RootJsonFormat[Instant] {
    override def write(obj: Instant): JsValue = JsString(obj.toString)

    override def read(json: JsValue): Instant = json match {
      case JsString(value) => Try(Instant.parse(value)).getOrElse(throw DeserializationException(CreatedDateNotSpecifiedAsISO8601(json).errorMessage))
      case j => throw DeserializationException(CreatedDateNotSpecifiedAsISO8601(j).errorMessage)
    }
  }


  implicit object ContactFormat extends RootJsonFormat[List[ContactMethod]] {
    def read(json: JsValue): List[ContactMethod] = {
      val deserializationExceptionMessage = ContactMissingContactOption.errorMessage
      json match {
        case JsObject(fields) if fields.isDefinedAt("email") || fields.isDefinedAt("slackChannel") =>
          val email = fields.get("email") match {
            case Some(JsString(value)) =>
              Some(Email(value))
            case _ =>
              None
          }
          val slackChannel = fields.get("slackChannel") match {
            case Some(JsString(value)) =>
              Some(Slack(value))
            case _ =>
              None
          }
          val contactMethods: List[ContactMethod] = List(email, slackChannel).flatten
          if (contactMethods.nonEmpty) contactMethods else throw DeserializationException(deserializationExceptionMessage)
        case _ =>
          throw DeserializationException(deserializationExceptionMessage)
      }
    }

    def write(obj: List[ContactMethod]): JsValue = {
      val map = obj.map {
        case Email(address) =>
          ("email", JsString(address))
        case Slack(channel) =>
          ("slackChannel", JsString(channel))
        case _ => ("unspecified", JsString("unspecified"))
      }.toMap
      JsObject(map)
    }
  }

  implicit object StreamTypeFormat extends RootJsonFormat[StreamType] {
    def read(json: JsValue): StreamType = json match {
      case JsString("Notification") => Notification
      case JsString("History") => History
      case JsString("CurrentState") => CurrentState
      case JsString("Telemetry") => Telemetry
      case _ =>
        import scala.reflect.runtime.{universe => ru}
        val tpe = ru.typeOf[StreamType]
        val knownDirectSubclasses: Set[ru.Symbol] = tpe.typeSymbol.asClass.knownDirectSubclasses
        throw DeserializationException(StreamTypeInvalid(json, knownDirectSubclasses).errorMessage)
    }

    def write(obj: StreamType): JsValue = {
      JsString(obj.toString)
    }
  }

  implicit object SchemasFormat extends RootJsonFormat[Schemas] {
    override def write(obj: Schemas): JsValue = {
      JsObject(Map[String, JsValue](
        "key" -> new SchemaFormat("key").write(obj.key),
        "value" -> new SchemaFormat("value").write(obj.value)
      ))
    }

    override def read(json: JsValue): Schemas = json match {
      case j: JsObject =>
        val key = Try(new SchemaFormat("key").read(j.getFields("key").headOption.getOrElse(JsObject.empty)))
        val value = Try(new SchemaFormat("value").read(j.getFields("value").headOption.getOrElse(JsObject.empty)))
        (key, value) match {
          case (Success(keySchema), Success(valueSchema)) =>
            Schemas(keySchema, valueSchema)
          case _ =>
            val errorMessage = List(key,value).filter(_.isFailure).flatMap {
              case Success(_) => None
              case Failure(exception) => Some(exception.getMessage)
            }.mkString(" ")
            throw DeserializationException(errorMessage)
        }
      case j => throw DeserializationException(InvalidSchemas(j).errorMessage)
    }
  }

  class SchemaFormat(field: String) extends RootJsonFormat[Schema] {
    override def write(obj: Schema): JsValue = {
      import spray.json._
      obj.toString().parseJson
    }

    override def read(json: JsValue): Schema = {
      val jsonString = json.compactPrint
      Try(new Schema.Parser().parse(jsonString)).getOrElse(throw DeserializationException(InvalidSchema(json, field).errorMessage))
    }
  }

  implicit object TopicMetadataV2Format extends RootJsonFormat[TopicMetadataV2Request] {
    override def write(obj: TopicMetadataV2Request): JsValue = jsonFormat9(TopicMetadataV2Request).write(obj)

    override def read(json: JsValue): TopicMetadataV2Request = json match {
      case j: JsObject =>
        val subject = validateSubject(Try(getStringWithKey(j, "subject")))
        val schemas = toMVR(Validated.catchNonFatal(SchemasFormat.read(j.getFields("schemas").headOption.getOrElse(throwDeserializationError("schemas", "JsObject with key and value Avro Schemas")))))
        val streamType = toMVR(Validated.catchNonFatal(StreamTypeFormat.read(j.getFields("streamType").headOption.getOrElse(throwDeserializationError("streamType", "String")))))
        val deprecated = toMVR(Validated.catchNonFatal(getBoolWithKey(j, "deprecated")))
        val dataClassification = toMVR(Validated.catchNonFatal(getStringWithKey(j, "dataClassification")))
        val contact = toMVR(Validated.catchNonFatal(ContactFormat.read(j.getFields("contact").headOption.getOrElse(throwDeserializationError("contact", "JsObject")))))
        val createdDate = toMVR(Validated.catchNonFatal(InstantFormat.read(j.getFields("createdDate").headOption.getOrElse(throwDeserializationError("createdDate", "ISO-8601 DateString formatted YYYY-MM-DDThh:mm:ssZ")))))
        val parentSubjects = toMVR(Validated.catchNonFatal(j.getFields("parentSubjects").headOption.map(_.convertTo[List[String]]).getOrElse(throwDeserializationError("parentSubjects", "List of String"))))
        val notes = toMVR(Validated.catchNonFatal(j.getFields("notes").headOption.map(_.convertTo[String])))
        (
          subject,
          schemas,
          streamType,
          deprecated,
          dataClassification,
          contact,
          createdDate,
          parentSubjects,
          notes
          ).mapN(TopicMetadataV2Request) match {
          case Valid(topicMetadataRequest) => topicMetadataRequest
          case Invalid(e) => throw DeserializationException(e.map(_.errorMessage).mkString_(" "))
        }
      case j =>
        import spray.json._
        val json =
          """
            |{
            |   "subject": "String a-zA-Z0-9_.-\",
            |   "schemas": {
            |     "key": {},
            |     "value": {}
            |   },
            |   "streamType": "oneOf History, Notification, Telemetry, CurrentState",
            |   "deprecated": false,
            |   "dataClassification": "Public",
            |   "contact": {
            |     "slackChannel" : "#channelName",
            |     "email" : "email@address.com"
            |   },
            |   "createdDate":"2020-01-20T12:34:56Z",
            |   "parentSubjects": ["subjectName"],
            |   "notes": "Optional - String Note"
            |}
            |""".stripMargin
        throw DeserializationException(s"Expected payload like ${json.parseJson.compactPrint}, but received ${j.compactPrint}")
    }
  }

  private def throwDeserializationError(key: String, `type`: String) = throw DeserializationException(MissingField(key, `type`).errorMessage)

  private def getBoolWithKey(json: JsObject, key: String): Boolean = {
    json.getFields(key).headOption.map(_.convertTo[Boolean]).getOrElse(throwDeserializationError(key, "Boolean"))
  }

  private def getStringWithKey(json: JsObject, key: String): String = {
    json.getFields(key).headOption.map(_.convertTo[String]).getOrElse(throwDeserializationError(key, "String"))
  }
}

sealed trait TopicMetadataV2Validator extends HydraSubjectValidator with Errors {

  def toMVR[A](v: Validated[Throwable, A]): MetadataValidationResult[A] = v.toValidatedNec.leftMap[NonEmptyChain[ExceptionThrownOnParseWithException]]{ es =>
    es.map(e => ExceptionThrownOnParseWithException(e.getMessage))
  }

  def validateSubject(subject: Try[String]): MetadataValidationResult[String] = {
    subject match {
      case Failure(exception) =>
        ExceptionThrownOnParseWithException(exception.getMessage).invalidNec
      case Success(value) =>
        validateSubjectCharacters(value) match {
          case Valid(a) =>
            a.validNec
          case Invalid(e) => ExternalValidator(e.head.errorMessage).invalidNec
        }
    }
  }

  sealed trait TopicMetadataV2PayloadValidation {
    def errorMessage: String
  }

  final case class ExceptionThrownOnParseWithException(message: String) extends TopicMetadataV2PayloadValidation {
    def errorMessage: String = message
  }

  final case class ExternalValidator(message: String) extends TopicMetadataV2PayloadValidation {
    override def errorMessage: String = message
  }

  type MetadataValidationResult[A] = ValidatedNec[TopicMetadataV2PayloadValidation, A]

}

sealed trait Errors {
  final case class CreatedDateNotSpecifiedAsISO8601(value: JsValue) {
    def errorMessage: String = s"Field `createdDate` expected ISO-8601 DateString formatted YYYY-MM-DDThh:mm:ssZ, received ${value.compactPrint}."
  }

  final case class InvalidSchema(value: JsValue, field: String) {
    def errorMessage: String = s"${value.compactPrint} is not a properly formatted Avro Schema for field `$field`."
  }

  final case class InvalidSchemas(value: JsValue) {
    def errorMessage: String = s"Field Schemas must be an object containing a `key` avro schema and a `value` avro schema, received ${value.compactPrint}."
  }

  final case class IncompleteSchemas(combinedErrorMessages: String) {
    def errorMessage: String = combinedErrorMessages
  }

  case object ContactMissingContactOption {
    def errorMessage: String = """Field `contact` expects one or more of `email` or `slackChannel`."""
  }

  import scala.reflect.runtime.{universe => ru}
  final case class StreamTypeInvalid(value: JsValue, knownDirectSubclasses: Set[ru.Symbol]) {
    def errorMessage: String = s"Field `streamType` expected oneOf $knownDirectSubclasses, received ${value.compactPrint}"
  }

  final case class MissingField(field: String, `type`: String) {
    def errorMessage: String = ""//s"Field of type $`type` is required in the payload."
  }

}
