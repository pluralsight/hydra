package hydra.ingest.modules

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, IO, Sync, Timer}
import cats.syntax.all._
import fs2.kafka.{Headers, Timestamp}
import hydra.avro.registry.SchemaRegistry
import hydra.ingest.app.AppConfig.{ConsumerOffsetsOffsetsTopicConfig, DVSConsumersTopicConfig, MetadataTopicsConfig, TagsConfig}
import hydra.kafka.algebras.KafkaAdminAlgebra.Topic
import hydra.kafka.algebras.KafkaClientAlgebra.{ConsumerGroup, Offset, Partition, PublishError, PublishResponse, TopicName}
import hydra.kafka.algebras.{KafkaAdminAlgebra, KafkaClientAlgebra, MetadataAlgebra}
import hydra.kafka.model.{ContactMethod, TopicMetadataV2, TopicMetadataV2Key}
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.programs.{CreateTopicProgram, KeyAndValueSchemaV2Validator}
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.TopicPartition
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import retry.RetryPolicies

class BootstrapSpec extends AnyWordSpecLike with Matchers {

  implicit private def unsafeLogger[F[_]: Sync]: SelfAwareStructuredLogger[F] =
    Slf4jLogger.getLogger[F]

  implicit private val timer: Timer[IO] =
    IO.timer(concurrent.ExecutionContext.global)

  implicit private val cs: ContextShift[IO] = IO.contextShift(concurrent.ExecutionContext.global)
  implicit private val c: ConcurrentEffect[IO] = IO.ioConcurrentEffect

  private val metadataSubjectV1 = Subject.createValidated("dvs.metadata.v1").get
  private val metadataSubjectV2 = Subject.createValidated("dvs.metadata").get
  private val consumersTopicSubject = Subject.createValidated("dvs.consumers-topic").get
  private val cooTopicSubject = Subject.createValidated("dvs.consumer-offsets-offsets").get

  private def createTestCase(
      metadataConfig: MetadataTopicsConfig,
      consumersTopicConfig: DVSConsumersTopicConfig,
      consumerOffsetsOffsetsTopicConfig: ConsumerOffsetsOffsetsTopicConfig,
      tagsTopicConfig: TagsConfig

  ): IO[(List[Topic], List[String], List[(GenericRecord, Option[GenericRecord], Option[Headers])])] = {
    val retry = RetryPolicies.alwaysGiveUp[IO]
    for {
      schemaRegistry <- SchemaRegistry.test[IO]
      kafkaAdmin <- KafkaAdminAlgebra.test[IO]()
      ref <- Ref[IO].of(List.empty[(GenericRecord, Option[GenericRecord], Option[Headers])])
      kafkaClient = new TestKafkaClientAlgebraWithPublishTo(ref)
      metadata <- MetadataAlgebra.make(metadataSubjectV2, "consumer_group",kafkaClient, schemaRegistry, consumeMetadataEnabled = true)
      c = CreateTopicProgram.make[IO](
        schemaRegistry,
        kafkaAdmin,
        kafkaClient,
        retry,
        metadataSubjectV2,
        metadata
      )
      boot <- Bootstrap.make[IO](c, metadataConfig, consumersTopicConfig, consumerOffsetsOffsetsTopicConfig, kafkaAdmin, tagsTopicConfig)
      _ <- boot.bootstrapAll
      topicCreated <- kafkaAdmin.describeTopic(metadataSubjectV2.value)
      topicCreated1 <- kafkaAdmin.describeTopic(metadataSubjectV1.value)
      topicCreated2 <- kafkaAdmin.describeTopic(consumersTopicConfig.topicName.value)
      topicCreated3 <- kafkaAdmin.describeTopic(consumerOffsetsOffsetsTopicConfig.topicName.value)
      topics = List(topicCreated, topicCreated1, topicCreated2, topicCreated3).flatten
      schemasAdded <- schemaRegistry.getAllSubjects
      messagesPublished <- ref.get
    } yield (topics, schemasAdded, messagesPublished)
  }

  "Bootstrap" must {
    val consumersTopicConfig =
      DVSConsumersTopicConfig(
        consumersTopicSubject,
        ContactMethod.create("test@test.com").get,
        1,
        1,
        1
      )

    val consumerOffsetsTopicConfig =
      ConsumerOffsetsOffsetsTopicConfig(
        cooTopicSubject,
        ContactMethod.create("test@test.com").get,
        1,
        1,
        1
      )
    val tagsTopicConfig =
      TagsConfig("", "_hydra.tags-topic", "")

    "create the metadata topics, consumers topic, and consumerOffsetsOffsets topic" in {
      val config =
        MetadataTopicsConfig(
          metadataSubjectV1,
          metadataSubjectV2,
          createV1OnStartup = true,
          createV2OnStartup = true,
          createV2TopicsEnabled = true,
          ContactMethod.create("test@test.com").get,
          1,
          1,
          1,
          "consumerGroup"
        )
      createTestCase(config, consumersTopicConfig, consumerOffsetsTopicConfig, tagsTopicConfig)
        .map {
          case (topicsCreated, schemasAdded, messagesPublished) =>
            topicsCreated should contain allOf(Topic(metadataSubjectV1.value, 1), Topic(metadataSubjectV2.value, 1),
              Topic(consumersTopicSubject.value, 1), Topic(cooTopicSubject.value, 1))
            schemasAdded should contain allOf (
              metadataSubjectV2.value + "-key",
              metadataSubjectV2.value + "-value",
              consumersTopicSubject.value + "-key",
              consumersTopicSubject.value + "-value",
              cooTopicSubject.value + "-key",
              cooTopicSubject.value + "-value")
            messagesPublished should have length 4
        }
        .unsafeRunSync()
    }

    "not create the metadata topic" in {
      val config =
        MetadataTopicsConfig(
          metadataSubjectV1,
          metadataSubjectV2,
          createV1OnStartup = false,
          createV2OnStartup = false,
          createV2TopicsEnabled = false,
          ContactMethod.create("test@test.com").get,
          1,
          1,
          1,
          "consumerGroup"
        )

      createTestCase(config, consumersTopicConfig, consumerOffsetsTopicConfig, tagsTopicConfig)
        .map {
          case (topicsCreated, schemasAdded, messagesPublished) =>
            topicsCreated should not contain Topic(metadataSubjectV2.value, 1)
            schemasAdded should contain noneOf (metadataSubjectV2.value + "-key", metadataSubjectV2.value + "-value")
            val keySchema = TopicMetadataV2.getSchemas.unsafeRunSync().key
            messagesPublished.flatMap(m => TopicMetadataV2Key.codec.decode(m._1, keySchema).toOption.map(_.subject)) should not contain metadataSubjectV2
        }
        .unsafeRunSync()
    }
  }

  private final class TestKafkaClientAlgebraWithPublishTo(publishTo: Ref[IO, List[(GenericRecord, Option[GenericRecord], Option[Headers])]]
  ) extends KafkaClientAlgebra[IO] {
    override def publishMessage(
        record: (GenericRecord, Option[GenericRecord], Option[Headers]),
		topicName: TopicName): IO[Either[PublishError, PublishResponse]] = {

      publishTo.update(_ :+ record).map(_ => PublishResponse(0, 0)).attemptNarrow[PublishError]
    }

    override def consumeMessages(topicName: TopicName, consumerGroup: String, commitOffsets: Boolean): fs2.Stream[IO, (GenericRecord, Option[GenericRecord], Option[Headers])] = fs2.Stream.empty

    override def publishStringKeyMessage(record: (Option[String], Option[GenericRecord], Option[Headers]), topicName: TopicName): IO[Either[PublishError, PublishResponse]] = ???

    override def consumeStringKeyMessages(topicName: TopicName, consumerGroup: ConsumerGroup, commitOffsets: Boolean): fs2.Stream[IO, (Option[String], Option[GenericRecord], Option[Headers])] = ???

    override def withProducerRecordSizeLimit(sizeLimitBytes: Long): IO[KafkaClientAlgebra[IO]] = ???

    override def consumeMessagesWithOffsetInfo(topicName: TopicName, consumerGroup: ConsumerGroup, commitOffsets: Boolean): fs2.Stream[IO, ((GenericRecord, Option[GenericRecord], Option[Headers]), (Partition, Offset))] = fs2.Stream.empty

    override def consumeStringKeyMessagesWithOffsetInfo(topicName: TopicName, consumerGroup: ConsumerGroup, commitOffsets: Boolean): fs2.Stream[IO, ((Option[String], Option[GenericRecord], Option[Headers]), (Partition, Offset))] = fs2.Stream.empty

    override def streamStringKeyFromGivenPartitionAndOffset(topicName: TopicName, consumerGroup: ConsumerGroup, commitOffsets: Boolean, topicPartitionAndOffsets: List[(TopicPartition, Offset)]): fs2.Stream[IO, ((Option[String], Option[GenericRecord], Option[Headers]), (Partition, Offset), Timestamp)] = ???

    override def streamAvroKeyFromGivenPartitionAndOffset(topicName: TopicName, consumerGroup: ConsumerGroup, commitOffsets: Boolean, topicPartitionAndOffsets: List[(TopicPartition, Offset)]): fs2.Stream[IO, ((GenericRecord, Option[GenericRecord], Option[Headers]), (Partition, Offset), Timestamp)] = ???
  }

}
