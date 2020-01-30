package hydra.kafka.serializers

import java.time.{Instant, ZoneOffset}

import org.scalatest.{Matchers, WordSpec}
import spray.json.JsString

class TopicMetadataV2ParserSpec extends WordSpec with Matchers with TopicMetadataV2Parser {
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
