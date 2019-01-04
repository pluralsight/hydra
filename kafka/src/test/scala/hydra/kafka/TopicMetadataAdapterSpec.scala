package hydra.kafka

import java.util.UUID

import hydra.kafka.model.{TopicMetadata, TopicMetadataAdapter}
import org.joda.time.DateTime
import org.scalatest.{FlatSpecLike, Matchers}

class TopicMetadataAdapterSpec extends Matchers
  with FlatSpecLike
  with TopicMetadataAdapter {

  "The TopicMetadataAdapter" should "build a resource" in {

    val uuid = UUID.randomUUID()

    val tm = TopicMetadata(
      id = uuid,
      schemaId = 1,
      streamType = "Notification",
      subject = "hydra-test",
      derived = false,
      dataClassification = "public",
      contact = "alex",
      additionalDocumentation = None,
      notes = None,
      createdDate = DateTime.now
    )

    val resource = toResource(tm)
    val md = resource.asJsObject
    md.fields("_links").asJsObject.fields("self").asJsObject.fields("href").convertTo[String] shouldBe s"/streams/${uuid.toString}"
    md.fields("_links").asJsObject.fields("hydra-schema").asJsObject.fields("href").convertTo[String] shouldBe "/schemas/hydra-test"
  }
}
