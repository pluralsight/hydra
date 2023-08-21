package hydra.ingest.utils

import cats.data.NonEmptyList
import cats.effect.IO
import cats.implicits._
import hydra.avro.registry.SchemaRegistry
import hydra.avro.registry.SchemaRegistry.SchemaId
import hydra.kafka.algebras.MetadataAlgebra.TopicMetadataContainer
import hydra.kafka.algebras.TestMetadataAlgebra
import hydra.kafka.model.ContactMethod.Email
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model._
import org.apache.avro.{Schema, SchemaBuilder}

import java.time.Instant

object TopicUtils {

  def updateTopicMetadata(topics: List[String], metadataAlgebra: TestMetadataAlgebra[IO], createdDate: Instant): IO[List[Unit]] = {
    topics.traverse(topic => {
      val keySchema = SchemaBuilder.record(topic + "Key").fields.requiredInt("test").endRecord()
      val valueSchema = SchemaBuilder.record(topic + "Value").fields.requiredInt("test").endRecord()
      val topicMetadataKey = TopicMetadataV2Key(Subject.createValidated(topic).get)
      val topicMetadataV2Request = TopicMetadataV2Request(
        Schemas(keySchema, valueSchema),
        StreamTypeV2.Entity,
        deprecated = false,
        deprecatedDate = None,
        replacementTopics = None,
        previousTopics = None,
        Public,
        NonEmptyList.of(Email.create("test@test.com").get),
        createdDate,
        List.empty,
        None,
        Some("dvs-teamName"),
        None,
        List.empty,
        Some("notificationUrl"),
        additionalValidations = None
      )
      val topicMetadataContainer = TopicMetadataContainer(
        topicMetadataKey,
        topicMetadataV2Request.toValue,
        keySchema.some,
        valueSchema.some
      )

      metadataAlgebra.addMetadata(topicMetadataContainer)
    })
  }
}