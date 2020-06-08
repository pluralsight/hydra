package hydra.kafka.marshallers

import java.time.Instant

import cats.data.NonEmptyList
import hydra.core.marshallers.History
import hydra.kafka.algebras.MetadataAlgebra.TopicMetadataV2Container
import hydra.kafka.model.ContactMethod.Slack
import hydra.kafka.model.TopicMetadataV2Transport.Subject
import hydra.kafka.model.{Public, TopicMetadataV2Adapter, TopicMetadataV2Key, TopicMetadataV2Value}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TopicMetadataV2AdapterSpec extends AnyWordSpec with Matchers with TopicMetadataV2Adapter {
  "V2AdapterSpec" should {
    "serialize MetadataV2Container" in {
      val date = Instant.now
      val key = TopicMetadataV2Key(Subject.createValidated("mySubjectHere").get)
      val value = TopicMetadataV2Value(History, deprecated = false, Public, NonEmptyList.one(Slack.create("#channel").get), date, List(), None)
      val container = TopicMetadataV2Container(key, Some(value))

      toResource(container).compactPrint shouldBe s"""{"_links":{"self":{"href":"/v2/streams/mySubjectHere"},"hydra-schema":{"href":"/v2/schemas/mySubjectHere"}},"key":{"subject":"mySubjectHere"},"value":{"contact":{"slackChannel":"#channel"},"createdDate":"${date.toString}","dataClassification":"Public","deprecated":false,"parentSubjects":[],"streamType":"History"}}"""
    }
  }
}
