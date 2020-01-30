package hydra.kafka.serializers

import java.time.ZoneOffset

import hydra.core.marshallers._
import hydra.kafka.model.{Email, Slack}
import org.scalatest.{Matchers, WordSpec}

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
      val invalidJsString = JsString("invalid date String")
      the [DeserializationException] thrownBy {
        InstantFormat.read(invalidJsString)
      } should have message (CreatedDateNotSpecifiedAsISO8601(invalidJsString).errorMessage)
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
      val json = "{}"
      val jsValue = json.parseJson
      the [DeserializationException] thrownBy {
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
      val jsValue = JsString("Not A Real Type")
      import scala.reflect.runtime.{universe => ru}
      val tpe = ru.typeOf[StreamType]
      val knownDirectSubclasses: Set[ru.Symbol] = tpe.typeSymbol.asClass.knownDirectSubclasses

      the [DeserializationException] thrownBy {
        StreamTypeFormat.read(jsValue)
      } should have message StreamTypeInvalid(jsValue, knownDirectSubclasses).errorMessage
    }

    "parse a valid schema" in {
      val jsonString =
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
          |""".stripMargin
      val jsValue = jsonString.parseJson
      SchemaFormat.read(jsValue).getName shouldBe "SomeName"
    }

    "throw an error given an invalid schema" in {
      val jsonString = "{}"
      val jsValue = jsonString.parseJson
      the [DeserializationException] thrownBy {
        SchemaFormat.read(jsValue).getName
      } should have message InvalidSchema(jsValue).errorMessage
    }


    //    "accept all fields" in {
//      val jsonData =
//        s"""
//          |{
//          | "contact": {
//          |   "slackChannel": "#foo",
//          |   "email": "foo@example.com"
//          | },
//          | "parentSubjects": ["foo"],
//          | "subject": "foo",
//          | "schemas": {
//          |   "key": {},
//          |   "value": {}
//          | },
//          | "streamType": "History",
//          | "dataClassification": "Public",
//          | "notes": "Hello world",
//          | "createdDate": "${Instant.now.toString}",
//          | "deprecated": false
//          |}
//          |""".stripMargin
//    }
  }
}
