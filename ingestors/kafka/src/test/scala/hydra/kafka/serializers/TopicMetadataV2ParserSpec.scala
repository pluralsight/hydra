package hydra.kafka.serializers

import java.time.Instant

import cats.data.NonEmptyList
import hydra.core.marshallers._
import hydra.kafka.model.ContactMethod.{Email, Slack}
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model._
import hydra.kafka.serializers.Errors._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

class TopicMetadataV2ParserSpec extends AnyWordSpecLike with Matchers {
  import TopicMetadataV2Parser._
  import spray.json._

  val validAvroSchema =
    """
      |{
      |  "namespace": "_hydra.metadata",
      |  "name": "SomeName",
      |  "type": "record",
      |  "version": 1,
      |  "fields": [
      |    {
      |      "name": "id",
      |      "type": "string"
      |    }
      |  ]
      |}
      |""".stripMargin.parseJson

  "TopicMetadataV2Deserializer" must {

    "return instant.now" in {
      InstantFormat
        .read(JsNull)
        .toEpochMilli shouldBe (Instant.now.toEpochMilli +- 10.seconds.toMillis)
    }

    "parse list of contact method with email and slack channel" in {
      val email = "dataplatform@pluralsight.com"
      val slackChannel = "#dev-data-platform"
      val json =
        s"""
          |{
          | "email":"$email",
          | "slackChannel":"$slackChannel"
          |}
          |""".stripMargin
      val jsValue = json.parseJson
      ContactFormat.read(jsValue).toList should contain allOf (Slack
        .create(
          slackChannel
        )
        .get, Email.create(email).get)
    }

    "parse list of contact method with only slack channel" in {
      val slackChannel = "#dev-data-platform"
      val json =
        s"""
           |{
           | "slackChannel":"$slackChannel"
           |}
           |""".stripMargin
      val jsValue = json.parseJson
      val contactList = ContactFormat.read(jsValue)
      contactList.head shouldBe Slack.create(slackChannel).get
      contactList should have length 1
    }

    "parse list of contact method with only an email" in {
      val email = "dataplatform@pluralsight.com"
      val json =
        s"""
           |{
           | "email":"$email"
           |}
           |""".stripMargin
      val jsValue = json.parseJson
      val contactList = ContactFormat.read(jsValue)
      contactList.head shouldBe Email.create(email).get
      contactList should have length 1
    }

    "throw error when parsing list of contact method with no required fields" in {
      val jsValue = JsObject.empty
      the[DeserializationException] thrownBy {
        ContactFormat.read(jsValue)
      } should have message ContactMissingContactOption.errorMessage
    }

    "parse one of each type of StreamType" in {
      StreamTypeFormat.read(JsString("Notification")) shouldBe Notification
      StreamTypeFormat.read(JsString("History")) shouldBe History
      StreamTypeFormat.read(JsString("CurrentState")) shouldBe CurrentState
      StreamTypeFormat.read(JsString("Telemetry")) shouldBe Telemetry
    }

    "throw error when parsing StreamType" in {
      val jsValue = JsString.empty
      import scala.reflect.runtime.{universe => ru}
      val tpe = ru.typeOf[StreamType]
      val knownDirectSubclasses: Set[ru.Symbol] =
        tpe.typeSymbol.asClass.knownDirectSubclasses

      the[DeserializationException] thrownBy {
        StreamTypeFormat.read(jsValue)
      } should have message StreamTypeInvalid(jsValue, knownDirectSubclasses).errorMessage
    }

    "parse one of each type of DataClassification" in {
      DataClassificationFormat.read(JsString("Public")) shouldBe Public
      DataClassificationFormat.read(JsString("InternalUseOnly")) shouldBe InternalUseOnly
      DataClassificationFormat.read(JsString("ConfidentialPII")) shouldBe ConfidentialPII
      DataClassificationFormat.read(JsString("RestrictedFinancial")) shouldBe RestrictedFinancial
      DataClassificationFormat.read(JsString("RestrictedEmployeeData")) shouldBe RestrictedEmployeeData
    }

    "throw error when parsing DataClassification" in {
      val jsValue = JsString.empty
      import scala.reflect.runtime.{universe => ru}
      val tpe = ru.typeOf[DataClassification]
      val knownDirectSubclasses: Set[ru.Symbol] =
        tpe.typeSymbol.asClass.knownDirectSubclasses

      the[DeserializationException] thrownBy {
        DataClassificationFormat.read(jsValue)
      } should have message DataClassificationInvalid(
        jsValue,
        knownDirectSubclasses
      ).errorMessage
    }

    "parse a valid schema" in {
      val jsValue = validAvroSchema
      new SchemaFormat(isKey = false).read(jsValue).getName shouldBe "SomeName"
    }

    "throw an error given an invalid schema" in {
      val jsValue = JsObject.empty
      the[DeserializationException] thrownBy {
        new SchemaFormat(isKey = false).read(jsValue).getName
      } should have message InvalidSchema(jsValue, isKey = false).errorMessage
    }

    "parse a valid Schemas object" in {
      val json =
        s"""
          |{
          | "key":${validAvroSchema.compactPrint},
          |"value":${validAvroSchema.compactPrint}
          |}
          |""".stripMargin.parseJson
      SchemasFormat.read(json) shouldBe Schemas(
        new SchemaFormat(isKey = true).read(validAvroSchema),
        new SchemaFormat(isKey = false).read(validAvroSchema)
      )
    }

    "throw a comprehensive error given an incomplete Schemas object" in {
      val errorMessage = IncompleteSchemas(
        List(
          InvalidSchema(JsObject.empty, isKey = true).errorMessage,
          InvalidSchema(JsObject.empty, isKey = false).errorMessage
        ).mkString(" ")
      ).errorMessage
      the[DeserializationException] thrownBy {
        val json = JsObject.empty
        SchemasFormat.read(json)
      } should have message errorMessage
    }

    "throw a generic schemas error given a random non JsObject" in {
      the[DeserializationException] thrownBy {
        val json = JsString.empty
        SchemasFormat.read(json)
      } should have message InvalidSchemas(JsString.empty).errorMessage
    }

    "parse a complete object and return a TopicMetadataV2Request" in {
      val (
        jsonData,
        subject,
        streamType,
        deprecated,
        dataClassification,
        email,
        slackChannel,
        parentSubjects,
        notes
      ) =
        createJsValueOfTopicMetadataV2Request(
          Subject.createValidated("Foo").get,
          "#slack_channel",
          "email@address.com"
        )()
      val tmv2 = TopicMetadataV2Format.read(jsonData)

      tmv2 shouldBe
        TopicMetadataV2Request(
          subject,
          Schemas(
            new SchemaFormat(isKey = true).read(validAvroSchema),
            new SchemaFormat(isKey = false).read(validAvroSchema)
          ),
          streamType,
          deprecated,
          dataClassification,
          NonEmptyList(email, slackChannel :: Nil),
          tmv2.createdDate,
          parentSubjects,
          notes
        )
    }

    "parse a complete object with no optional fields and return a TopicMetadataV2Request" in {
      val (
        jsonData,
        subject,
        streamType,
        _,
        dataClassification,
        email,
        slackChannel,
        _,
        notes
      ) =
        createJsValueOfTopicMetadataV2Request(
          Subject.createValidated("Foo").get,
          "#slack_channel",
          "email@address.com",
          allOptionalFieldsPresent = false
        )()
      val tmv2 = TopicMetadataV2Format.read(jsonData)

      tmv2 shouldBe
        TopicMetadataV2Request(
          subject,
          Schemas(
            new SchemaFormat(isKey = true).read(validAvroSchema),
            new SchemaFormat(isKey = false).read(validAvroSchema)
          ),
          streamType,
          deprecated = false,
          dataClassification,
          NonEmptyList(email, slackChannel :: Nil),
          tmv2.createdDate,
          parentSubjects = List(),
          notes
        )
    }

    "throw deserialization error with invalid payload" in {
      the[DeserializationException] thrownBy {
        TopicMetadataV2Format.read(JsString.empty)
      } should have message invalidPayloadProvided(JsString.empty)
    }

    def containsAllOf(error: Throwable, errorMessages: String*) =
      errorMessages.forall(error.getMessage.contains)

    "accumulate the errors from an empty object payload" in {
      val error = the[DeserializationException] thrownBy {
        TopicMetadataV2Format.read(JsObject.empty)
      }
      assert(
        containsAllOf(
          error,
          "Field `schemas`",
          "Field `streamType`",
          "Field `dataClassification`",
          "Field `contact`"
        )
      )
    }

  }

  private def createJsValueOfTopicMetadataV2Request(
      subject: Subject,
      slackChannel: String,
      email: String,
      allOptionalFieldsPresent: Boolean = true
  )(
      streamType: StreamType = History,
      deprecated: Boolean = false,
      dataClassification: DataClassification = Public,
      validAvroSchema: JsValue = validAvroSchema,
      parentSubjects: List[Subject] = List(),
      notes: Option[String] = None,
      createdDate: Instant = Instant.now()
  ): (
      JsValue,
      Subject,
      StreamType,
      Boolean,
      DataClassification,
      Email,
      Slack,
      List[Subject],
      Option[String]
  ) = {
    val jsValue = s"""
         |{
         |  "subject": "${subject.value}",
         |  "schemas": {
         |   "key": ${validAvroSchema.compactPrint},
         |   "value": ${validAvroSchema.compactPrint}
         |  },
         |  "streamType": "${streamType.toString}",
         |  "dataClassification":"${dataClassification.toString}",
         |  "contact": {
         |    "slackChannel": "$slackChannel",
         |    "email": "$email"
         |  }
         |  ${if (allOptionalFieldsPresent) {
                       s""","parentSubjects": ${parentSubjects.toJson.compactPrint},"deprecated":$deprecated,"createdDate":"${createdDate.toString}""""
                     } else ""}
         |  ${if (notes.isDefined) s""","notes": "${notes.get}"""" else ""}}
         |""".stripMargin.parseJson
    (
      jsValue,
      subject,
      streamType,
      deprecated,
      dataClassification,
      Email.create(email).get,
      Slack.create(slackChannel).get,
      parentSubjects,
      notes
    )
  }

  "TopicMetadataV2Serializer" must {

    "serialize a subject as a string" in {
      val subjectName = "ValidSubjectName"
      val subject = Subject.createValidated(subjectName).get
      SubjectFormat.write(subject) shouldBe JsString(subjectName)
    }

    "serialize an instant" in {
      val dateString = "2020-02-02T12:34:56Z"
      val instant = Instant.parse(dateString)
      InstantFormat.write(instant) shouldBe JsString(dateString)
    }

    "serialize a list of contactMethod objects" in {
      val email = Email.create("some@address.com").get
      val slack = Slack.create("#this_is_my_slack_channel").get

      ContactFormat.write(NonEmptyList(email, Nil)) shouldBe JsObject(
        Map("email" -> JsString(email.address.value))
      )
      ContactFormat.write(NonEmptyList(slack, Nil)) shouldBe JsObject(
        Map("slackChannel" -> JsString(slack.channel.value))
      )
      val jObject = JsObject(
        Map(
          "email" -> JsString(email.address.value),
          "slackChannel" -> JsString(slack.channel.value)
        )
      )
      ContactFormat.write(NonEmptyList(email, slack :: Nil)) shouldBe jObject
      jObject.compactPrint shouldBe s"""{"email":"${email.address}","slackChannel":"${slack.channel}"}"""
    }

    "serialize a StreamType" in {
      val streamType = History
      StreamTypeFormat.write(streamType) shouldBe JsString("History")
    }

    "serialize a DataClassificationFormat" in {
      DataClassificationFormat.write(Public) shouldBe JsString("Public")
    }

    "serialize an avro schema" in {
      val schema = new SchemaFormat(isKey = true).read(validAvroSchema)
      new SchemaFormat(isKey = true)
        .write(schema)
        .compactPrint shouldBe validAvroSchema.compactPrint
    }

    "serialize the Schemas object" in {
      val keySchema = new SchemaFormat(isKey = true).read(validAvroSchema)
      val valueSchema = new SchemaFormat(isKey = false).read(validAvroSchema)
      SchemasFormat.write(Schemas(keySchema, valueSchema)) shouldBe JsObject(
        Map(
          "key" -> new SchemaFormat(true).write(keySchema),
          "value" -> new SchemaFormat(false).write(valueSchema)
        )
      )
    }

    "serialize the entire topicMetadata Request payload" in {
      val subject = Subject.createValidated("some_valid_subject_name").get
      val keySchema = new SchemaFormat(isKey = true).read(validAvroSchema)
      val valueSchema = new SchemaFormat(isKey = false).read(validAvroSchema)
      val streamType = History
      val deprecated = false
      val dataClassification = Public
      val email = Email.create("some@address.com").get
      val slack = Slack.create("#valid_slack_channel").get
      val contact = NonEmptyList(email, slack :: Nil)
      val createdDate = Instant.now
      val parentSubjects = List(
        Subject.createValidated("valid_parent_1").get,
        Subject.createValidated("valid_parent_2").get
      )
      val notes = Some("Notes go here.")

      val topicMetadataV2 = TopicMetadataV2Request(
        subject = subject,
        schemas = Schemas(
          keySchema,
          valueSchema
        ),
        streamType = streamType,
        deprecated = deprecated,
        dataClassification = dataClassification,
        contact = contact,
        createdDate = createdDate,
        parentSubjects = parentSubjects,
        notes = notes
      )

      TopicMetadataV2Format.write(topicMetadataV2) shouldBe
        createJsValueOfTopicMetadataV2Request(
          subject,
          slack.channel.value,
          email.address.value
        )(
          streamType,
          deprecated,
          dataClassification,
          validAvroSchema,
          parentSubjects,
          notes,
          createdDate
        )._1
    }

  }
}
