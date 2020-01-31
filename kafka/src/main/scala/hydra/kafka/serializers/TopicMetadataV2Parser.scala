package hydra.kafka.serializers

import java.time.Instant

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyChain, Validated, ValidatedNec}
import cats.implicits._
import hydra.avro.resource.HydraSubjectValidator
import hydra.core.marshallers.{CurrentState, GenericServiceResponse, History, Notification, StreamType, Telemetry}
import hydra.kafka.model.{ContactMethod, Email, Schemas, Slack, TopicMetadataV2Request}
import hydra.kafka.serializers.Errors._
import org.apache.avro.Schema
import spray.json.{DefaultJsonProtocol, DeserializationException, JsArray, JsBoolean, JsObject, JsString, JsValue, RootJsonFormat}

import scala.util.{Failure, Success, Try}

trait TopicMetadataV2Parser extends SprayJsonSupport with DefaultJsonProtocol with TopicMetadataV2Validator {
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
              val emailRegex = """^[A-Z0-9a-z._%+-]+@[A-Za-z0-9.-]+.[A-Za-z]{2,64}$""".r
              if (emailRegex.pattern.matcher(value).matches()) Right(Some(Email(value))) else Left(InvalidEmailProvided(JsString(value)).errorMessage)
            case _ =>
              Right(None)
          }
          val slackChannel = fields.get("slackChannel") match {
            case Some(JsString(value)) =>
              val slackRegex = """^[#][^\sA-Z]{1,79}$""".r
              if (slackRegex.pattern.matcher(value).matches()) Right(Some(Slack(value))) else Left(InvalidSlackChannelProvided(JsString(value)).errorMessage)
            case _ =>
              Right(None)
          }
          val contacts = List(email, slackChannel)
          if (contacts.forall(_.isRight)) {
            val contactMethods = contacts.flatMap{ case Right(value) => value}
            if (contactMethods.nonEmpty) contactMethods else throw DeserializationException(deserializationExceptionMessage)
          } else {
            val errorString = contacts.foldLeft(List[String]())((acc, i) => acc ++ (i match {
              case Right(_) => None
              case Left(value) => Some(value)
            })).mkString(" ")
            throw DeserializationException(errorString)
          }
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
        "key" -> new SchemaFormat(isKey = true).write(obj.key),
        "value" -> new SchemaFormat(isKey = false).write(obj.value)
      ))
    }

    override def read(json: JsValue): Schemas = json match {
      case j: JsObject =>
        val key = Try(new SchemaFormat(isKey = true).read(j.getFields("key").headOption.getOrElse(JsObject.empty)))
        val value = Try(new SchemaFormat(isKey = false).read(j.getFields("value").headOption.getOrElse(JsObject.empty)))
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

  class SchemaFormat(isKey: Boolean) extends RootJsonFormat[Schema] {
    override def write(obj: Schema): JsValue = {
      import spray.json._
      obj.toString().parseJson
    }

    override def read(json: JsValue): Schema = {
      val jsonString = json.compactPrint
      Try(new Schema.Parser().parse(jsonString)).getOrElse(throw DeserializationException(InvalidSchema(json, isKey).errorMessage))
    }
  }

  implicit object TopicMetadataV2Format extends RootJsonFormat[TopicMetadataV2Request] {
    override def write(obj: TopicMetadataV2Request): JsValue = jsonFormat9(TopicMetadataV2Request).write(obj)

    override def read(json: JsValue): TopicMetadataV2Request = json match {
      case j: JsObject =>
        val subject = validateSubject(Try(getStringWithKey(j, "subject")))
        val schemas = toResult(SchemasFormat.read(j.getFields("schemas").headOption.getOrElse(throwDeserializationError("schemas", "JsObject with key and value Avro Schemas"))))
        val streamType = toResult(StreamTypeFormat.read(j.getFields("streamType").headOption.getOrElse(throwDeserializationError("streamType", "String"))))
        val deprecated = toResult(getBoolWithKey(j, "deprecated"))
        val dataClassification = toResult(getStringWithKey(j, "dataClassification"))
        val contact = toResult(ContactFormat.read(j.getFields("contact").headOption.getOrElse(throwDeserializationError("contact", "JsObject"))))
        val createdDate = toResult(InstantFormat.read(j.getFields("createdDate").headOption.getOrElse(throwDeserializationError("createdDate", "ISO-8601 DateString formatted YYYY-MM-DDThh:mm:ssZ"))))
        val parentSubjects = toResult(j.getFields("parentSubjects").headOption.map(_.convertTo[List[String]]).getOrElse(throwDeserializationError("parentSubjects", "List of String")))
        val notes = toResult(j.getFields("notes").headOption.map(_.convertTo[String]))
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
        throw DeserializationException(s"Expected payload like ${json.parseJson.prettyPrint}, but received ${j.prettyPrint}")
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

sealed trait TopicMetadataV2Validator extends HydraSubjectValidator {

  def toResult[A](a: => A): MetadataValidationResult[A] = {
    val v = Validated.catchNonFatal(a)
    v.toValidatedNec.leftMap[NonEmptyChain[ExceptionThrownOnParseWithException]]{ es =>
      es.map(e => ExceptionThrownOnParseWithException(e.getMessage))
    }
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

object Errors {
  final case class CreatedDateNotSpecifiedAsISO8601(value: JsValue) {
    def errorMessage: String = s"Field `createdDate` expected ISO-8601 DateString formatted YYYY-MM-DDThh:mm:ssZ, received ${value.compactPrint}."
  }

  final case class InvalidEmailProvided(value: JsValue) {
    def errorMessage: String = s"Field `email` not recognized as a valid address, received ${value.compactPrint}."
  }

  final case class InvalidSlackChannelProvided(value: JsValue) {
    def errorMessage: String = s"Field `slackChannel` must be all lowercase with no spaces and less than 80 characters, received ${value.compactPrint}."
  }

  final case class InvalidSchema(value: JsValue, isKey: Boolean) {
    def errorMessage: String = s"${value.compactPrint} is not a properly formatted Avro Schema for field `${if (isKey) "key" else "value"}`."
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

  final case class MissingField(field: String, fieldType: String)
  {
    def errorMessage: String = s"Field `$field` of type $fieldType"
  }
}
