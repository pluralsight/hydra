package hydra.ingest.modules

import cats.effect.concurrent.Ref
import cats.effect.{IO, Sync, Timer}
import cats.implicits._
import hydra.avro.registry.SchemaRegistry
import hydra.ingest.app.AppConfig.V2MetadataTopicConfig
import hydra.kafka.algebras.KafkaAdminAlgebra.Topic
import hydra.kafka.algebras.KafkaClientAlgebra.{ConsumerGroup, PublishError, TopicName}
import hydra.kafka.algebras.{KafkaAdminAlgebra, KafkaClientAlgebra}
import hydra.kafka.model.ContactMethod
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

  private val metadataSubject = Subject.createValidated("metadata").get

  private def createTestCase(
      config: V2MetadataTopicConfig
  ): IO[(Option[Topic], List[String], Map[String, (GenericRecord, GenericRecord)])] = {
    val retry = RetryPolicies.alwaysGiveUp[IO]
    for {
      schemaRegistry <- SchemaRegistry.test[IO]
      kafkaAdmin <- KafkaAdminAlgebra.test[IO]
      ref <- Ref[IO].of(Map.empty[String, (GenericRecord, GenericRecord)])
      kafkaClient = new TestKafkaClientAlgebraWithPublishTo(ref)
      c = new CreateTopicProgram[IO](
        schemaRegistry,
        kafkaAdmin,
        kafkaClient,
        retry,
        metadataSubject
      )
      boot <- Bootstrap.make[IO](c, config)
      _ <- boot.bootstrapAll
      topicCreated <- kafkaAdmin.describeTopic(metadataSubject.value)
      schemasAdded <- schemaRegistry.getAllSubjects
      messagesPublished <- ref.get
    } yield (topicCreated, schemasAdded, messagesPublished)
  }

  "Bootstrap" must {
    "create the metadata topic" in {
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
      createTestCase(config)
        .map {
          case (topicCreated, schemasAdded, messagesPublished) =>
            topicCreated shouldBe Some(Topic(metadataSubject.value, 1))
            schemasAdded should contain allOf (metadataSubject.value + "-key", metadataSubject.value + "-value")
            messagesPublished.keys.toList should have length 1
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
      createTestCase(config)
        .map {
          case (topicCreated, schemasAdded, messagesPublished) =>
            topicCreated should not be defined
            schemasAdded shouldBe empty
            messagesPublished shouldBe empty
        }
        .unsafeRunSync()
    }
  }

  private final class TestKafkaClientAlgebraWithPublishTo(publishTo: Ref[IO, Map[String, (GenericRecord, GenericRecord)]]
  ) extends KafkaClientAlgebra[IO] {
    override def publishMessage(
        record: (GenericRecord, GenericRecord),
        topicName: TopicName): IO[Either[PublishError, Unit]] =
      publishTo.update(_ + (topicName -> record)).attemptNarrow[PublishError]

    override def consumeMessages(topicName: TopicName, consumerGroup: String): fs2.Stream[IO, (GenericRecord, GenericRecord)] = fs2.Stream.empty

    override def publishStringKeyMessage(record: (String, GenericRecord), topicName: TopicName): IO[Either[PublishError, Unit]] = ???

    override def consumeStringKeyMessages(topicName: TopicName, consumerGroup: ConsumerGroup): fs2.Stream[IO, (String, GenericRecord)] = ???
  }

}
