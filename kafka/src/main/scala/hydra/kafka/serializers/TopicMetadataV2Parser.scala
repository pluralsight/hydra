package hydra.kafka.serializers

import java.time.Instant

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import cats.data.Validated.{Invalid, Valid}
import cats.data.ValidatedNec
import cats.implicits._
import hydra.avro.resource.HydraSubjectValidator
import hydra.core.marshallers.{CurrentState, GenericServiceResponse, History, Notification, StreamType, Telemetry}
import hydra.kafka.model.{ContactMethod, Email, Schemas, Slack, TopicMetadataV2Request}
import org.apache.avro.Schema
import spray.json.{DefaultJsonProtocol, DeserializationException, JsBoolean, JsObject, JsString, JsValue, RootJsonFormat}

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
        "key" -> SchemaFormat.write(obj.key),
        "value" -> SchemaFormat.write(obj.value)
      ))
    }

    override def read(json: JsValue): Schemas = json match {
      case j: JsObject =>
        val key = Try(SchemaFormat.read(j.getFields("key").headOption.getOrElse(JsString.empty)))
        val value = Try(SchemaFormat.read(j.getFields("value").headOption.getOrElse(JsString.empty)))
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
    }
  }

  implicit object SchemaFormat extends RootJsonFormat[Schema] {
    override def write(obj: Schema): JsValue = {
      import spray.json._
      obj.toString().parseJson
    }

    override def read(json: JsValue): Schema = {
      val jsonString = json.compactPrint
      Try(new Schema.Parser().parse(jsonString)).getOrElse(throw DeserializationException(InvalidSchema(json).errorMessage))
    }
  }

  implicit object TopicMetadataV2Format extends RootJsonFormat[TopicMetadataV2Request] {
    override def write(obj: TopicMetadataV2Request): JsValue = jsonFormat9(TopicMetadataV2Request).write(obj)

    override def read(json: JsValue): TopicMetadataV2Request = json match {
      case j: JsObject =>
        val subject = validateSubject(Try(getStringWithKey(j, "subject")))
        val schemas = validateSimpleExceptionThrow(Try(SchemasFormat.read(j)))
        val streamType = validateSimpleExceptionThrow(Try(StreamTypeFormat.read(j.getFields("streamType").headOption.getOrElse(JsString.empty))))
        val deprecated = validateSimpleExceptionThrow(Try(getBoolWithKey(j, "deprecated")))
        val dataClassification = validateSimpleExceptionThrow(Try(getStringWithKey(j, "dataClassification")))
        throw DeserializationException("WTF did you send us?")
      case _ =>
        throw DeserializationException("WTF did you send us?")
    }
  }

  private def getBoolWithKey(json: JsObject, key: String): Boolean = {
    json.getFields(key) match {
      case JsBoolean(bool) :: _ => bool
      case _ => throw DeserializationException("Field `deprecated` must be passed as a boolean value.")
    }
  }

  private def getStringWithKey(json: JsObject, key: String): String = {
    json.getFields(key) match {
      case JsString(value) :: _ => value
      case _ => throw DeserializationException(s"Field `$key` must be passed as a string value.")
    }
  }
}

sealed trait TopicMetadataV2Validator extends HydraSubjectValidator {

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

  def validateSimpleExceptionThrow[A](genericTry: Try[A]): MetadataValidationResult[A] = {
    genericTry match {
      case Failure(exception) => ExceptionThrownOnParseWithException(exception.getMessage).invalidNec
      case Success(value) => value.validNec
    }
  }


  sealed trait DomainValidation {
    def errorMessage: String
  }

  final case class ExceptionThrownOnParseWithException(message: String) extends DomainValidation {
    def errorMessage: String = message
  }

  final case class CreatedDateNotSpecifiedAsISO8601(value: JsValue) extends DomainValidation {
    def errorMessage: String = s"Field `createdDate` expected ISO-8601 DateString formatted YYYY-MM-DDThh:mm:ssZ, received $value."
  }

  final case class InvalidSchema(value: JsValue) extends DomainValidation {
    def errorMessage: String = s"${value.compactPrint} is not a properly formatted Avro Schema."
  }

  case object ContactMissingContactOption extends DomainValidation {
    def errorMessage: String = """Field `contact` expects one or more of `email` or `slackChannel`."""
  }

  import scala.reflect.runtime.{universe => ru}
  final case class StreamTypeInvalid(value: JsValue, knownDirectSubclasses: Set[ru.Symbol]) extends DomainValidation {
    def errorMessage: String = s"Field `streamType` expected oneOf $knownDirectSubclasses, received $value"
  }

  final case class ExternalValidator(message: String) extends DomainValidation {
    override def errorMessage: String = message
  }


  type MetadataValidationResult[A] = ValidatedNec[DomainValidation, A]

}
