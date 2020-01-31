package hydra.kafka.serializers

import java.time.{Instant, ZoneOffset}

import hydra.core.marshallers._
import hydra.kafka.model.{Email, Schemas, Slack, TopicMetadataV2Request}
import org.scalatest.{Matchers, WordSpec}
import Errors._
import hydra.avro.resource.HydraSubjectValidator

class TopicMetadataV2ParserSpec extends WordSpec with Matchers with TopicMetadataV2Parser {
  import spray.json._

  "TopicMetadataV2Parser" must {

    "parse a valid ISO-8601 date in Zulu time" in {
      val instant = InstantFormat.read(JsString("2020-01-20T12:34:56Z")).atOffset(ZoneOffset.UTC)
      instant.getMonth.getValue shouldBe 1
      instant.getDayOfMonth shouldBe 20
      instant.getYear shouldBe 2020
      instant.getHour shouldBe 12
      instant.getMinute shouldBe 34
      instant.getSecond shouldBe 56
    }

    "throw a Deserialization error with invalid date string" in {
      val invalidJsString = JsString.empty
      the[DeserializationException] thrownBy {
        InstantFormat.read(invalidJsString)
      } should have message CreatedDateNotSpecifiedAsISO8601(invalidJsString).errorMessage
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
      ContactFormat.read(jsValue) should contain allOf(Slack(slackChannel), Email(email))
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
      contactList.head shouldBe Slack(slackChannel)
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
      contactList.head shouldBe Email(email)
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
      val knownDirectSubclasses: Set[ru.Symbol] = tpe.typeSymbol.asClass.knownDirectSubclasses

      the[DeserializationException] thrownBy {
        StreamTypeFormat.read(jsValue)
      } should have message StreamTypeInvalid(jsValue, knownDirectSubclasses).errorMessage
    }

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
      SchemasFormat.read(json) shouldBe Schemas(new SchemaFormat(isKey = true).read(validAvroSchema),new SchemaFormat(isKey = false).read(validAvroSchema))
    }

    "throw a comprehensive error given an incomplete Schemas object" in {
      val errorMessage = IncompleteSchemas(
          List(InvalidSchema(JsObject.empty, isKey = true).errorMessage,InvalidSchema(JsObject.empty, isKey = false).errorMessage)
            .mkString(" ")
        ).errorMessage
      the[DeserializationException] thrownBy{
        val json = JsObject.empty
        SchemasFormat.read(json)
      } should have message errorMessage
    }

    "parse a complete object and return a TopicMetadataV2Request" in {
      val (jsonData, subject, streamType, deprecated, dataClassification, email, slackChannel, createdDateString, parentSubjects, notes) =
        createJsValueOfTopicMetadataV2Request("Foo","#slack_channel","email@address.com","2020-01-20T12:34:56Z")
      TopicMetadataV2Format.read(jsonData) shouldBe
        TopicMetadataV2Request(
          subject,
          Schemas(new SchemaFormat(isKey = true).read(validAvroSchema),new SchemaFormat(isKey = false).read(validAvroSchema)),
          streamType,
          deprecated,
          dataClassification,
          List(email, slackChannel),
          Instant.parse(createdDateString),
          parentSubjects,
          Some(notes)
        )
    }

    def containsAllOf(error: Throwable, errorMessages: String*) = errorMessages.forall(error.getMessage.contains)

    "accumulate the errors from an empty object payload" in {
      val error = the[DeserializationException] thrownBy {
        TopicMetadataV2Format.read(JsObject.empty)
      }
      assert(containsAllOf(error,"Field `subject`", "Field `schemas`", "Field `streamType`","Field `deprecated`","Field `dataClassification`","Field `contact`","Field `createdDate`","Field `parentSubjects`"))
    }

    "accumulate errors from improper provided data" in {
      val (jsonData, _, _, _, _, email, slack, createdDate, _, _) =
        createJsValueOfTopicMetadataV2Request("@#$%^&","NOT a slack channel","invalid@address","2020-01-20")
      val error = the[DeserializationException] thrownBy {
        TopicMetadataV2Format.read(jsonData)
      }
      containsAllOf(error, HydraSubjectValidator.SubjectHasInvalidCharacters.errorMessage, Errors.CreatedDateNotSpecifiedAsISO8601(JsString(createdDate)).errorMessage, InvalidEmailProvided(JsString(email.address)).errorMessage, InvalidSlackChannelProvided(JsString(slack.channel)).errorMessage)
    }

  }

  private def createJsValueOfTopicMetadataV2Request(subject: String, slackChannel: String, email: String, createdDate: String):
                                        (JsValue, String, StreamType, Boolean, String, Email, Slack, String, List[String], String) = {
    val streamType = History
    val deprecated = false
    val dataClassification = "Public"
    val validAvroSchema =
    """{"namespace": "_hydra.metadata","name": "SomeName","type": "record","version": 1,"fields": [{"name": "id","type": "string"}]}""".parseJson
    val parentSubjects = List("1","2")
    val notes = "My Note Here"
    val jsValue = s"""
         |{
         |  "subject": "$subject",
         |  "schemas": {
         |   "key": ${validAvroSchema.compactPrint},
         |   "value": ${validAvroSchema.compactPrint}
         |  },
         |  "streamType": "${streamType.toString}",
         |  "deprecated": $deprecated,
         |  "dataClassification":"$dataClassification",
         |  "contact": {
         |    "slackChannel": "$slackChannel",
         |    "email": "$email"
         |  },
         |  "createdDate": "$createdDate",
         |  "parentSubjects": ${parentSubjects.toJson.compactPrint},
         |  "notes": "$notes"
         |}
         |""".stripMargin.parseJson
    (jsValue, subject, streamType, deprecated, dataClassification, Email(email), Slack(slackChannel), createdDate, parentSubjects, notes)
  }
}
