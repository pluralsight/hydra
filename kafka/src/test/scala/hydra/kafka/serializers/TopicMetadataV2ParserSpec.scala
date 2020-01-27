package hydra.kafka.serializers

import java.time.Instant

import org.scalatest.{Matchers, WordSpec}

class TopicMetadataV2ParserSpec extends WordSpec with Matchers {
  "TopicMetadataV2Parser" must {
    "should accept all fields" in {
      val jsonData =
        s"""
          |{
          | "contact": {
          |   "slackChannel": "#foo",
          |   "email": "foo@example.com"
          | },
          | "parentSubjects": ["foo"],
          | "subject": "foo",
          | "schemas": {
          |   "key": {},
          |   "value": {}
          | },
          | "streamType": "History",
          | "dataClassification": "Public",
          | "notes": "Hello world",
          | "createdDate": "${Instant.now.toString}",
          | "deprecated": false
          |}
          |""".stripMargin
    }
  }
}
