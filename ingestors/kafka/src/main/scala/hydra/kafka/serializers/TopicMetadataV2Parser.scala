package hydra.kafka.serializers

import java.time.Instant
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import cats.data.Validated.{Invalid, Valid}
import cats.data._
import cats.syntax.all._
import enumeratum.EnumEntry
import eu.timepit.refined.auto._
import hydra.kafka.model.ContactMethod.{Email, Slack}
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model._
import hydra.kafka.serializers.Errors._
import hydra.kafka.serializers.TopicMetadataV2Parser.IntentionallyUnimplemented
import org.apache.avro.Schema
import spray.json.{DefaultJsonProtocol, DeserializationException, JsObject, JsString, JsValue, RootJsonFormat}

import collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import spray.json.JsonFormat
import spray.json.JsNumber

object TopicMetadataV2Parser extends TopicMetadataV2Parser {
  case object IntentionallyUnimplemented extends RuntimeException
}

sealed trait TopicMetadataV2Parser
    extends SprayJsonSupport
    with DefaultJsonProtocol
    with TopicMetadataV2Validator {

  implicit object SubjectFormat extends RootJsonFormat[Subject] {

    override def write(obj: Subject): JsValue = {
      JsString(obj.value)
    }

    override def read(json: JsValue): Subject = json match {
      case JsString(value) =>
        Subject
          .createValidated(value)
          .getOrElse(
            throw DeserializationException(
              Subject.invalidFormat
            )
          )
      case j => throw DeserializationException(Subject.invalidFormat)
    }
  }

  implicit object InstantFormat extends RootJsonFormat[Instant] {
    override def write(obj: Instant): JsValue = JsString(obj.toString)

    override def read(json: JsValue): Instant = Instant.now()
  }

  implicit object ContactFormat
      extends RootJsonFormat[NonEmptyList[ContactMethod]] {

    def extractContactMethod[A](
        optionalField: Option[JsValue],
        f: String => Option[A],
        e: JsValue => String
    ): Either[String, Option[A]] = {
      optionalField match {
        case Some(JsString(value)) =>
          f(value) match {
            case Some(fval) => Right(Some(fval))
            case _          => Left(e(JsString(value)))
          }
        case _ =>
          Right(None)
      }
    }

    def separateEithers(
        email: Either[String, Option[Email]],
        slackChannel: Either[String, Option[Slack]]
    ): ValidatedNel[String, List[ContactMethod]] = {
      val e = Validated.fromEither(email).toValidatedNel
      val s = Validated.fromEither(slackChannel).toValidatedNel
      (e, s).mapN {
        case (Some(em), sl)   => List(em) ++ sl
        case (None, Some(sl)) => List(sl)
        case _                => List.empty
      }
    }

    def read(json: JsValue): NonEmptyList[ContactMethod] = {
      val deserializationExceptionMessage =
        ContactMissingContactOption.errorMessage
      json match {
        case JsObject(fields)
            if fields.isDefinedAt("email") || fields.isDefinedAt(
              "slackChannel"
            ) =>
          val email = extractContactMethod(
            fields.get("email"),
            Email.create,
            Errors.invalidEmailProvided
          )

          val slackChannel = extractContactMethod(
            fields.get("slackChannel"),
            Slack.create,
            Errors.invalidSlackChannelProvided
          )
          separateEithers(email, slackChannel) match {
            case Valid(contactMethods) =>
              NonEmptyList
                .fromList(contactMethods)
                .getOrElse(
                  throw DeserializationException(
                    deserializationExceptionMessage
                  )
                )
            case Invalid(e) =>
              val errorString = e.toList.mkString(" ")
              throw DeserializationException(errorString)
          }
        case _ =>
          throw DeserializationException(deserializationExceptionMessage)
      }
    }

    def write(obj: NonEmptyList[ContactMethod]): JsValue = {
      val map = obj
        .map {
          case Email(address) =>
            ("email", JsString(address))
          case Slack(channel) =>
            ("slackChannel", JsString(channel))
          case _ => ("unspecified", JsString("unspecified"))
        }
        .toList
        .toMap
      JsObject(map)
    }
  }

  implicit object StreamTypeV2Format
    extends RootJsonFormat[StreamTypeV2] {

    def read(json: JsValue): StreamTypeV2 = json match {
      case JsString("Entity")                 => StreamTypeV2.Entity
      case JsString("Event")                  => StreamTypeV2.Event
      case JsString("Telemetry")              => StreamTypeV2.Telemetry
      case _ =>
        import scala.reflect.runtime.{universe => ru}
        val tpe = ru.typeOf[StreamTypeV2]
        val knownDirectSubclasses: Set[ru.Symbol] =
          tpe.typeSymbol.asClass.knownDirectSubclasses
        throw DeserializationException(
          StreamTypeInvalid(json, knownDirectSubclasses).errorMessage
        )
    }

    def write(obj: StreamTypeV2): JsValue = {
      JsString(obj.toString)
    }
  }

  implicit object DataClassificationFormat
      extends RootJsonFormat[DataClassification] {

    def read(json: JsValue): DataClassification = json match {
      case JsString("Public")                 => Public
      case JsString("InternalUseOnly")        => InternalUseOnly
      case JsString("ConfidentialPII")        => ConfidentialPII
      case JsString("RestrictedFinancial")    => RestrictedFinancial
      case JsString("RestrictedEmployeeData") => RestrictedEmployeeData
      case _ =>
        import scala.reflect.runtime.{universe => ru}
        val tpe = ru.typeOf[DataClassification]
        val knownDirectSubclasses: Set[ru.Symbol] =
          tpe.typeSymbol.asClass.knownDirectSubclasses
        throw DeserializationException(
          DataClassificationInvalid(json, knownDirectSubclasses).errorMessage
        )
    }

    def write(obj: DataClassification): JsValue = {
      JsString(obj.toString)
    }
  }

  implicit object SchemasFormat extends RootJsonFormat[Schemas] {

    override def write(obj: Schemas): JsValue = {
      JsObject(
        Map[String, JsValue](
          "key" -> new SchemaFormat(isKey = true).write(obj.key),
          "value" -> new SchemaFormat(isKey = false).write(obj.value)
        )
      )
    }

    override def read(json: JsValue): Schemas = json match {
      case j: JsObject =>
        val key = Try(
          new SchemaFormat(isKey = true)
            .read(j.getFields("key").headOption.getOrElse(JsObject.empty))
        )
        val value = Try(
          new SchemaFormat(isKey = false)
            .read(j.getFields("value").headOption.getOrElse(JsObject.empty))
        )
        (key, value) match {
          case (Success(keySchema), Success(valueSchema)) =>
            Schemas(keySchema, valueSchema)
          case _ =>
            val errorMessage = List(key, value)
              .filter(_.isFailure)
              .flatMap {
                case Success(_)         => None
                case Failure(exception) => Some(exception.getMessage)
              }
              .mkString(" ")
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

    def isNamespaceInvalid(schema: Schema): Boolean = {
      val t = schema.getType
      t match {
        case Schema.Type.RECORD =>
          val currentNamespace = Option(schema.getNamespace).exists(f => !f.matches("^[A-Za-z0-9_\\.]*"))
          val allRecords = schema.getFields.asScala.toList.exists(f => isNamespaceInvalid(f.schema()))
          currentNamespace || allRecords
        case _ => false
      }
    }

    override def read(json: JsValue): Schema = {
      val jsonString = json.compactPrint
      val schema = try {
        new Schema.Parser().parse(jsonString)
      } catch {
        case e: Throwable => throw DeserializationException(InvalidSchema(json, isKey, Some(e)).errorMessage)
      }

      if(isNamespaceInvalid(schema)) {
        throw DeserializationException(InvalidSchema(json, isKey,
          Some(InvalidNamespace("Invalid character. Namespace must conform to regex ^[A-Za-z0-9_\\.]*"))).errorMessage)
      } else {
        schema
      }
    }
  }

  implicit val topicMetadataNumPartitionsFormat: JsonFormat[TopicMetadataV2Request.NumPartitions] = new RootJsonFormat[TopicMetadataV2Request.NumPartitions] {
    def read(json: JsValue): TopicMetadataV2Request.NumPartitions = {
      val int = json.convertTo[Int]
      TopicMetadataV2Request.NumPartitions.from(int) match {
        case Right(value) => value
        case Left(value) => throw new DeserializationException(value)
      }
    }
    
    def write(obj: TopicMetadataV2Request.NumPartitions): JsValue = JsNumber(obj.value)
    
  }

  class EnumEntryJsonFormat[E <: EnumEntry](values: Seq[E]) extends RootJsonFormat[E] {

    override def write(obj: E): JsValue = JsString(obj.entryName)

    override def read(json: JsValue): E = json match {
      case s: JsString => values.find(v => v.entryName == s.value).getOrElse(deserializationError(s))
      case x => deserializationError(x)
    }

    private def deserializationError(value: JsValue) = throw DeserializationException(s"Expected a value from enum $values instead of $value")
  }

  implicit val additionalValidationFormat: EnumEntryJsonFormat[AdditionalValidation] =
    new EnumEntryJsonFormat[AdditionalValidation](Seq.empty)

  implicit object TopicMetadataV2Format
      extends RootJsonFormat[TopicMetadataV2Request] {

    override def write(obj: TopicMetadataV2Request): JsValue =
      jsonFormat14(TopicMetadataV2Request.apply).write(obj)

    override def read(json: JsValue): TopicMetadataV2Request = json match {
      case j: JsObject =>
        val metadataValidationResult = MetadataOnlyRequestFormat.getValidationResult(json)
        val schemas = toResult(
          SchemasFormat.read(
            j.getFields("schemas")
              .headOption
              .getOrElse(
                throwDeserializationError(
                  "schemas",
                  "JsObject with key and value Avro Schemas"
                )
              )
          )
        )
        (schemas, metadataValidationResult) match {
          case (Valid(s), Valid(m)) => TopicMetadataV2Request.fromMetadataOnlyRequest(s, m)

          case (Invalid(es), Invalid(em)) =>
            throw DeserializationException(es.combine(em).map(_.errorMessage).mkString_(" "))

          case (Invalid(es), Valid(_)) =>
            throw DeserializationException(es.map(_.errorMessage).mkString_(" "))

          case (Valid(_), Invalid(em)) =>
            throw DeserializationException(em.map(_.errorMessage).mkString_(" "))
        }
      case j =>
        throw DeserializationException(invalidPayloadProvided(j))
    }
  }

  implicit object MetadataOnlyRequestFormat extends RootJsonFormat[MetadataOnlyRequest] {
    override def write(obj: MetadataOnlyRequest): JsValue = {
      JsString(obj.toString)
    }
    override def read(json: JsValue): MetadataOnlyRequest =  {
      getValidationResult(json) match {
          case Valid(metadataOnlyRequest) => metadataOnlyRequest
          case Invalid(e) =>
            throw DeserializationException(e.map(_.errorMessage).mkString_(" "))
        }
    }

    def getValidationResult(json: JsValue): MetadataValidationResult[MetadataOnlyRequest] = json match {
      case j: JsObject =>
        val subject = toResult(
          SubjectFormat
            .read(j.getFields("subject").headOption.getOrElse(JsString.empty))
        )
        val streamType = toResult(
          StreamTypeV2Format.read(
            j.getFields("streamType")
              .headOption
              .getOrElse(throwDeserializationError("streamType", "String"))
          )
        )
        val deprecated = toResult(getBoolWithKey(j, "deprecated"))
        val deprecatedDate = if ( deprecated.toOption.getOrElse(false) && !j.getFields("deprecatedDate").headOption.getOrElse(None).equals(None)) {
          toResult(Option(Instant.parse(j.getFields("deprecatedDate").headOption
            .getOrElse(throwDeserializationError("deprecatedDate","long"))
            .toString.replace("\"",""))))
        } else {
          toResult(None)
        }
        val dataClassification = toResult(
          DataClassificationFormat.read(
            j.getFields("dataClassification")
              .headOption
              .getOrElse(
                throwDeserializationError("dataClassification", "String")
              )
          )
        )
        val contact = toResult(
          ContactFormat.read(
            j.getFields("contact")
              .headOption
              .getOrElse(throwDeserializationError("contact", "JsObject"))
          )
        )
        val createdDate = toResult(Instant.now())
        val parentSubjects = toResult(
          j.fields.get("parentSubjects") match {
            case Some(t) => t.convertTo[Option[List[String]]].getOrElse(List.empty)
            case None => List.empty[String]
          })
        val notes = toResult(
          j.getFields("notes").headOption.map(_.convertTo[String])
        )
        val teamName = toResult(
          j.fields.get("teamName") match {
            case Some(teamName) => teamName.convertTo[Option[String]]
            case None => throwDeserializationError("teamName", "String")
          }
        )
        val numPartitions = toResult(
          j.fields.get("numPartitions").map { num =>
            TopicMetadataV2Request.NumPartitions.from(num.convertTo[Int]).toOption match {
              case Some(numP) => numP
              case None => throwDeserializationError("numPartitions", "Int [10-50]")
            }
          }
        )
        val tags = toResult(
          j.fields.get("tags") match {
            case Some(t) => t.convertTo[Option[List[String]]].getOrElse(List.empty)
            case None => List.empty[String]
          }
        )
        val notificationUrl = toResult(
          j.getFields("notificationUrl").headOption.map(_.convertTo[String])
        )
        (
          streamType,
          deprecated,
          deprecatedDate,
          dataClassification,
          contact,
          createdDate,
          parentSubjects,
          notes,
          teamName,
          numPartitions,
          tags,
          notificationUrl,
          toResult(None) // Never pick additionalValidations from the request.
          ).mapN(MetadataOnlyRequest.apply)
    }
  }

  implicit object MaybeSchemasFormat extends RootJsonFormat[MaybeSchemas] {
    override def read(json: JsValue): MaybeSchemas = throw IntentionallyUnimplemented

    override def write(obj: MaybeSchemas): JsValue =  {
      val keyJson = ("key" -> obj.key.map(k => new SchemaFormat(isKey = true).write(k)).getOrElse(JsString("Unable to retrieve Key Schema")))
      val valueJson = ("value" -> obj.value.map(v => new SchemaFormat(isKey = false).write(v)).getOrElse(JsString("Unable to retrieve Value Schema")))

      JsObject(
        List(keyJson, valueJson).toMap
      )
    }
  }

  implicit object TopicMetadataResponseV2Format extends RootJsonFormat[TopicMetadataV2Response] {
    override def read(json: JsValue): TopicMetadataV2Response = throw IntentionallyUnimplemented

    override def write(obj: TopicMetadataV2Response): JsValue = jsonFormat13(TopicMetadataV2Response.apply).write(obj)
  }

  private def throwDeserializationError(key: String, `type`: String) =
    throw DeserializationException(MissingField(key, `type`).errorMessage)

  private def getBoolWithKey(json: JsObject, key: String): Boolean = {
    json
      .getFields(key)
      .headOption
      .exists(_.convertTo[Boolean])
  }
}

sealed trait TopicMetadataV2Validator {

  def toResult[A](a: => A): MetadataValidationResult[A] = {
    val v = Validated.catchNonFatal(a)
    v.toValidatedNec
      .leftMap[NonEmptyChain[ExceptionThrownOnParseWithException]] { es =>
        es.map(e => ExceptionThrownOnParseWithException(e.getMessage))
      }
  }

  type MetadataValidationResult[A] =
    ValidatedNec[TopicMetadataV2PayloadValidation, A]

}

sealed trait TopicMetadataV2PayloadValidation {
  def errorMessage: String
}

final case class ExceptionThrownOnParseWithException(message: String)
    extends TopicMetadataV2PayloadValidation {
  override def errorMessage: String = message
}

object Errors {

  def invalidPayloadProvided(actual: JsValue): String = {
    import spray.json._
    val expected =
      """
        |{
        |   "subject": "String a-zA-Z0-9.-\\",
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

  def invalidEmailProvided(value: JsValue) =
    s"Field `email` not recognized as a valid address, received ${value.compactPrint}."

  def invalidSlackChannelProvided(value: JsValue) =
    s"Field `slackChannel` must be all lowercase with no spaces and less than 80 characters, received ${value.compactPrint}."

  final case class InvalidSchema(value: JsValue, isKey: Boolean, error: Option[Throwable] = none) {

    def errorMessage: String = {
      s"${value.compactPrint} is not a properly formatted Avro Schema for field `${if (isKey) "key" else "value"}`." +
        s"${error.map(e => s"\nError: ${e.getMessage}\n").getOrElse("")}"
    }
  }

  final case class InvalidNamespace(reason: String) extends Throwable {
    override def getMessage: String = {
      s"One or more of the Namespaces provided are invalid due to: $reason"
    }
  }

  final case class InvalidSchemas(value: JsValue) {

    def errorMessage: String =
      s"Field Schemas must be an object containing a `key` avro schema and a `value` avro schema, received ${value.compactPrint}."
  }

  final case class IncompleteSchemas(combinedErrorMessages: String) {
    def errorMessage: String = combinedErrorMessages
  }

  case object ContactMissingContactOption {

    def errorMessage: String =
      """Field `contact` expects one or more of `email` or `slackChannel`."""
  }

  import scala.reflect.runtime.{universe => ru}

  final case class StreamTypeInvalid(
      value: JsValue,
      knownDirectSubclasses: Set[ru.Symbol]
  ) {

    def errorMessage: String =
      s"Field `streamType` expected oneOf $knownDirectSubclasses, received ${value.compactPrint}"
  }

  import scala.reflect.runtime.{universe => ru}

  final case class DataClassificationInvalid(
      value: JsValue,
      knownDirectSubclasses: Set[ru.Symbol]
  ) {

    def errorMessage: String =
      s"Field `dataClassification` expected oneOf $knownDirectSubclasses, received ${value.compactPrint}"
  }

  final case class MissingField(field: String, fieldType: String) {
    def errorMessage: String = s"Field `$field` of type $fieldType"
  }

}
