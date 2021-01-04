package hydra.ingest.modules

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, IO, Sync, Timer}
import cats.syntax.all._
import fs2.kafka.Headers
import hydra.avro.registry.SchemaRegistry
import hydra.ingest.app.AppConfig.{ConsumerOffsetsOffsetsTopicConfig, DVSConsumersTopicConfig, V2MetadataTopicConfig}
import hydra.kafka.algebras.KafkaAdminAlgebra.Topic
import hydra.kafka.algebras.KafkaClientAlgebra.{ConsumerGroup, Offset, Partition, PublishError, PublishResponse, TopicName}
import hydra.kafka.algebras.{KafkaAdminAlgebra, KafkaClientAlgebra, MetadataAlgebra}
import hydra.kafka.model.{ContactMethod, TopicMetadataV2, TopicMetadataV2Key}
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.programs.CreateTopicProgram
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.avro.generic.GenericRecord
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

  private val metadataSubject = Subject.createValidated("dvs.metadata").get
  private val consumersTopicSubject = Subject.createValidated("dvs.consumers-topic").get
  private val cooTopicSubject = Subject.createValidated("dvs.consumer-offsets-offsets").get

  private def createTestCase(
      metadataConfig: V2MetadataTopicConfig,
      consumersTopicConfig: DVSConsumersTopicConfig,
      consumerOffsetsOffsetsTopicConfig: ConsumerOffsetsOffsetsTopicConfig

  ): IO[(List[Topic], List[String], List[(GenericRecord, Option[GenericRecord], Option[Headers])])] = {
    val retry = RetryPolicies.alwaysGiveUp[IO]
    for {
      schemaRegistry <- SchemaRegistry.test[IO]
      kafkaAdmin <- KafkaAdminAlgebra.test[IO]
      ref <- Ref[IO].of(List.empty[(GenericRecord, Option[GenericRecord], Option[Headers])])
      kafkaClient = new TestKafkaClientAlgebraWithPublishTo(ref)
      metadata <- MetadataAlgebra.make(metadataSubject.value, "consumer_group",kafkaClient, schemaRegistry, consumeMetadataEnabled = true)
      c = new CreateTopicProgram[IO](
        schemaRegistry,
        kafkaAdmin,
        kafkaClient,
        retry,
        metadataSubject,
        metadata
      )
      boot <- Bootstrap.make[IO](c, metadataConfig, consumersTopicConfig, consumerOffsetsOffsetsTopicConfig)
      _ <- boot.bootstrapAll
      topicCreated <- kafkaAdmin.describeTopic(metadataSubject.value)
      topicCreated2 <- kafkaAdmin.describeTopic(consumersTopicConfig.topicName.value)
      topicCreated3 <- kafkaAdmin.describeTopic(consumerOffsetsOffsetsTopicConfig.topicName.value)
      topics = List(topicCreated, topicCreated2, topicCreated3).flatten
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
        1
      )

    val consumerOffsetsTopicConfig =
      ConsumerOffsetsOffsetsTopicConfig(
        cooTopicSubject,
        ContactMethod.create("test@test.com").get,
        1,
        1
      )

    "create the metadata topic, consumers topic, and consumerOffsetsOffsets topic" in {
      val config =
        V2MetadataTopicConfig(
          metadataSubject,
          createOnStartup = true,
          createV2TopicsEnabled = true,
          ContactMethod.create("test@test.com").get,
          1,
          1,
          "consumerGroup"
        )
      createTestCase(config, consumersTopicConfig, consumerOffsetsTopicConfig)
        .map {
          case (topicsCreated, schemasAdded, messagesPublished) =>
            topicsCreated should contain allOf(Topic(metadataSubject.value, 1), Topic(consumersTopicSubject.value, 1), Topic(cooTopicSubject.value, 1))
            schemasAdded should contain allOf (
              metadataSubject.value + "-key",
              metadataSubject.value + "-value",
              consumersTopicSubject.value + "-key",
              consumersTopicSubject.value + "-value",
              cooTopicSubject.value + "-key",
              cooTopicSubject.value + "-value")
            messagesPublished should have length 3
        }
        .unsafeRunSync()
    }

    "not create the metadata topic" in {
      val config =
        V2MetadataTopicConfig(
          metadataSubject,
          createOnStartup = false,
          createV2TopicsEnabled = false,
          ContactMethod.create("test@test.com").get,
          1,
          1,
          "consumerGroup"
        )

      createTestCase(config, consumersTopicConfig, consumerOffsetsTopicConfig)
        .map {
          case (topicsCreated, schemasAdded, messagesPublished) =>
            topicsCreated should not contain Topic(metadataSubject.value, 1)
            schemasAdded should contain noneOf (metadataSubject.value + "-key", metadataSubject.value + "-value")
            val keySchema = TopicMetadataV2.getSchemas.unsafeRunSync().key
            messagesPublished.flatMap(m => TopicMetadataV2Key.codec.decode(m._1, keySchema).toOption.map(_.subject)) should not contain metadataSubject
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
  }

}
