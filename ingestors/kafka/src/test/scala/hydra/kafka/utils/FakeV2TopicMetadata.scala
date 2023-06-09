package hydra.kafka.utils

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

object FakeV2TopicMetadata {

  private def buildSchema(topic: String, upgrade: Boolean): Schema = {
    val schemaStart = SchemaBuilder
      .record("name" + topic.replace("-", "").replace(".", ""))
      .fields()
      .requiredString("id" + topic.replace("-", "").replace(".", ""))

    if (upgrade && !topic.contains("-key")) {
      schemaStart.nullableBoolean("upgrade", upgrade).endRecord()
    } else {
      schemaStart.endRecord()
    }
  }

  private def createTopicMetadataRequest(
                                          keySchema: Schema,
                                          valueSchema: Schema,
                                          email: String = "test@test.com",
                                          createdDate: Option[Instant] = None,
                                          deprecated: Boolean = false,
                                          deprecatedDate: Option[Instant] = None
                                        ): TopicMetadataV2Request =
    TopicMetadataV2Request(
      Schemas(keySchema, valueSchema),
      StreamTypeV2.Entity,
      deprecated = deprecated,
      deprecatedDate,
      Public,
      NonEmptyList.of(Email.create(email).get),
      createdDate.getOrElse(Instant.now()),
      List.empty,
      None,
      Some("dvs-teamName"),
      None,
      List.empty,
      Some("notificationUrl")
    )

  def writeV2TopicMetadata(
                            topics: List[String],
                            metadataAlgebra: TestMetadataAlgebra[IO],
                            createdDate: Option[Instant] = None
                          ): IO[List[Unit]] = {
    topics.traverse(topic => {
      val keySchema = buildSchema(topic + "-key", false)
      val valueSchema = buildSchema(topic + "-value", false)
      val topicMetadataKey =
        TopicMetadataV2Key(Subject.createValidated(topic).get)
      val req = createTopicMetadataRequest(
        keySchema,
        valueSchema,
        createdDate = createdDate
      )
      val topicMetadataContainer = TopicMetadataContainer(
        topicMetadataKey,
        req.toValue,
        keySchema.some,
        valueSchema.some
      )

      metadataAlgebra.addMetadata(topicMetadataContainer)
    })
  }

  def registerTopics(topicNames: List[String],
                     schemaAlgebra: SchemaRegistry[IO],
                     registerKey: Boolean,
                     upgrade: Boolean): IO[List[SchemaId]] = {
    topicNames
      .flatMap(topic =>
        if (registerKey) List(topic + "-key", topic + "-value") else List(topic + "-value"))
      .traverse(topic =>
        schemaAlgebra.registerSchema(topic, buildSchema(topic, upgrade)))
  }
}
