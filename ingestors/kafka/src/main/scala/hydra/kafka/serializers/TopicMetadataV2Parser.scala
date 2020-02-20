package hydra.kafka.serializers

import java.time.Instant

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyChain, NonEmptyList, Validated, ValidatedNec, ValidatedNel}
import cats.implicits._
import hydra.core.marshallers.{CurrentState, GenericServiceResponse, History, Notification, StreamType, Telemetry}
import hydra.kafka.model.{ConfidentialPII, ContactMethod, DataClassification, Email, InternalUseOnly, Public, RestrictedEmployeeData, RestrictedFinancial, Schemas, Slack, Subject, TopicMetadataV2Request}
import hydra.kafka.serializers.Errors._
import org.apache.avro.Schema
import spray.json.{DefaultJsonProtocol, DeserializationException, JsArray, JsBoolean, JsObject, JsString, JsValue, RootJsonFormat}

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

object TopicMetadataV2Parser extends TopicMetadataV2Parser

sealed trait TopicMetadataV2Parser extends SprayJsonSupport with DefaultJsonProtocol with TopicMetadataV2Validator {
  implicit object SubjectFormat extends RootJsonFormat[Subject] {
    override def write(obj: Subject): JsValue = {
      JsString(obj.value)
    }

    override def read(json: JsValue): Subject = json match {
      case JsString(value) => Subject.createValidated(value) match {
        case Some(subject) => subject
        case None => throw DeserializationException(InvalidSubject(json).errorMessage)
      }
      case j => throw DeserializationException(InvalidSubject(j).errorMessage)
    }
  }

  implicit object InstantFormat extends RootJsonFormat[Instant] {
    override def write(obj: Instant): JsValue = JsString(obj.toString)

    override def read(json: JsValue): Instant = json match {
      case JsString(value) => Try(Instant.parse(value)).getOrElse(throw DeserializationException(CreatedDateNotSpecifiedAsISO8601(json).errorMessage))
      case _ => throwDeserializationError("createdDate", "ISO-8601 DateString formatted YYYY-MM-DDThh:mm:ssZ")
    }
  }

  implicit object ContactFormat extends RootJsonFormat[NonEmptyList[ContactMethod]] {
    def extractContactMethod[A](optionalField: Option[JsValue], regex: Regex, f: String => A, e: JsValue => String): Either[String, Option[A]] = {
      optionalField match {
        case Some(JsString(value)) =>
          if (regex.pattern.matcher(value).matches()) Right(Some(f(value))) else Left(e(JsString(value)))
        case _ =>
          Right(None)
      }
    }

    def separateEithers(email: Either[String, Option[Email]], slackChannel: Either[String, Option[Slack]]): ValidatedNel[String, List[ContactMethod]] = {
      val e = Validated.fromEither(email).toValidatedNel
      val s = Validated.fromEither(slackChannel).toValidatedNel
      (e, s).mapN {
        case (Some(em), sl) => List(em) ++ sl
        case (None, Some(sl)) => List(sl)
        case _ => List.empty
      }
    }

    def read(json: JsValue): NonEmptyList[ContactMethod] = {
      val deserializationExceptionMessage = ContactMissingContactOption.errorMessage
      json match {
        case JsObject(fields) if fields.isDefinedAt("email") || fields.isDefinedAt("slackChannel") =>
          val emailRegex = """^[A-Z0-9a-z._%+-]+@[A-Za-z0-9.-]+.[A-Za-z]{2,64}$""".r
          val email = extractContactMethod(fields.get("email"), emailRegex, Email.apply, Errors.invalidEmailProvided)
          val slackRegex = """^[#][^\sA-Z]{1,79}$""".r
          val slackChannel = extractContactMethod(fields.get("slackChannel"), slackRegex, Slack.apply, Errors.invalidSlackChannelProvided)
          separateEithers(email, slackChannel) match {
            case Valid(contactMethods) =>
              NonEmptyList.fromList(contactMethods).getOrElse(throw DeserializationException(deserializationExceptionMessage))
            case Invalid(e) =>
              val errorString = e.toList.mkString(" ")
              throw DeserializationException(errorString)
          }
        case _ =>
          throw DeserializationException(deserializationExceptionMessage)
      }
    }

    def write(obj: NonEmptyList[ContactMethod]): JsValue = {
      val map = obj.map {
        case Email(address) =>
          ("email", JsString(address))
        case Slack(channel) =>
          ("slackChannel", JsString(channel))
        case _ => ("unspecified", JsString("unspecified"))
      }.toList.toMap
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

  implicit object DataClassificationFormat extends RootJsonFormat[DataClassification] {
    def read(json: JsValue): DataClassification = json match {
      case JsString("Public") => Public
      case JsString("InternalUseOnly") => InternalUseOnly
      case JsString("ConfidentialPII") => ConfidentialPII
      case JsString("RestrictedFinancial") => RestrictedFinancial
      case JsString("RestrictedEmployeeData") => RestrictedEmployeeData
      case _ =>
        import scala.reflect.runtime.{universe => ru}
        val tpe = ru.typeOf[DataClassification]
        val knownDirectSubclasses: Set[ru.Symbol] = tpe.typeSymbol.asClass.knownDirectSubclasses
        throw DeserializationException(DataClassificationInvalid(json, knownDirectSubclasses).errorMessage)
    }

    def write(obj: DataClassification): JsValue = {
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
        val subject = toResult(SubjectFormat.read(j.getFields("subject").headOption.getOrElse(JsString.empty)))
        val schemas = toResult(SchemasFormat.read(j.getFields("schemas").headOption.getOrElse(throwDeserializationError("schemas", "JsObject with key and value Avro Schemas"))))
        val streamType = toResult(StreamTypeFormat.read(j.getFields("streamType").headOption.getOrElse(throwDeserializationError("streamType", "String"))))
        val deprecated = toResult(getBoolWithKey(j, "deprecated"))
        val dataClassification = toResult(DataClassificationFormat.read(j.getFields("dataClassification").headOption.getOrElse(throwDeserializationError("dataClassification", "String"))))
        val contact = toResult(ContactFormat.read(j.getFields("contact").headOption.getOrElse(throwDeserializationError("contact", "JsObject"))))
        val createdDate = toResult(InstantFormat.read(j.getFields("createdDate").headOption.getOrElse(JsString.empty)))
        val parentSubjects = toResult(j.getFields("parentSubjects").headOption.map(_.convertTo[List[Subject]]).getOrElse(throwDeserializationError("parentSubjects", "List of Subject")))
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
        throw DeserializationException(invalidPayloadProvided(j))
    }
  }

  private def throwDeserializationError(key: String, `type`: String) = throw DeserializationException(MissingField(key, `type`).errorMessage)

  private def getBoolWithKey(json: JsObject, key: String): Boolean = {
    json.getFields(key).headOption.map(_.convertTo[Boolean]).getOrElse(throwDeserializationError(key, "Boolean"))
  }
}

sealed trait TopicMetadataV2Validator {
  def toResult[A](a: => A): MetadataValidationResult[A] = {
    val v = Validated.catchNonFatal(a)
    v.toValidatedNec.leftMap[NonEmptyChain[ExceptionThrownOnParseWithException]]{ es =>
      es.map(e => ExceptionThrownOnParseWithException(e.getMessage))
    }
  }

  type MetadataValidationResult[A] = ValidatedNec[TopicMetadataV2PayloadValidation, A]

}

sealed trait TopicMetadataV2PayloadValidation {
  def errorMessage: String
}
final case class ExceptionThrownOnParseWithException(message: String) extends TopicMetadataV2PayloadValidation {
  override def errorMessage: String = message
}


object Errors {
  final case class CreatedDateNotSpecifiedAsISO8601(value: JsValue) {
    def errorMessage: String = s"Field `createdDate` expected ISO-8601 DateString formatted YYYY-MM-DDThh:mm:ssZ, received ${value.compactPrint}."
  }

  def invalidPayloadProvided(actual: JsValue): String = {
    import spray.json._
    val expected =
      """
        |{
        |   "subject": "String a-zA-Z0-9_.-\\",
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

    s"Expected payload like ${expected.parseJson.prettyPrint}, but received ${actual.prettyPrint}"
  }

  def invalidEmailProvided(value: JsValue) = s"Field `email` not recognized as a valid address, received ${value.compactPrint}."

  def invalidSlackChannelProvided(value: JsValue) = s"Field `slackChannel` must be all lowercase with no spaces and less than 80 characters, received ${value.compactPrint}."


  final case class InvalidSchema(value: JsValue, isKey: Boolean) {
    def errorMessage: String = s"${value.compactPrint} is not a properly formatted Avro Schema for field `${if (isKey) "key" else "value"}`."
  }

  final case class InvalidSubject(jsValue: JsValue) {
    val errorMessage = s"Field `subject` must be a string containing only numbers, letters, hyphens, periods, and underscores, received ${jsValue.compactPrint}."
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

  import scala.reflect.runtime.{universe => ru}
  final case class DataClassificationInvalid(value: JsValue, knownDirectSubclasses: Set[ru.Symbol]) {
    def errorMessage: String = s"Field `dataClassification` expected oneOf $knownDirectSubclasses, received ${value.compactPrint}"
  }

  final case class MissingField(field: String, fieldType: String)
  {
    def errorMessage: String = s"Field `$field` of type $fieldType"
  }

}
