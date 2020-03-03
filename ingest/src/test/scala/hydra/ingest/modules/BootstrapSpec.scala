package hydra.ingest.modules

import cats.effect.concurrent.Ref
import cats.effect.{IO, Sync, Timer}
import hydra.avro.registry.SchemaRegistry
import hydra.ingest.app.AppConfig.V2MetadataTopicConfig
import hydra.kafka.model.ContactMethod
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.producer.KafkaRecord
import hydra.kafka.programs.CreateTopicProgram
import hydra.kafka.util.KafkaClient
import hydra.kafka.util.KafkaClient.{PublishError, Topic, TopicName}
import hydra.kafka.util.KafkaUtils.TopicDetails
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.scalatest.{Matchers, WordSpec}
import retry.RetryPolicies
import cats.implicits._

class BootstrapSpec extends WordSpec with Matchers {

  implicit private def unsafeLogger[F[_]: Sync]: SelfAwareStructuredLogger[F] =
    Slf4jLogger.getLogger[F]
  implicit private val timer: Timer[IO] =
    IO.timer(concurrent.ExecutionContext.global)

  private val metadataSubject = Subject.createValidated("metadata").get

  private def createTestCase(config: V2MetadataTopicConfig)
    : IO[(Option[Topic], List[String], List[KafkaRecord[_, _]])] = {
    val retry = RetryPolicies.alwaysGiveUp[IO]
    for {
      schemaRegistry <- SchemaRegistry.test[IO]
      underlying <- KafkaClient.test[IO]
      ref <- Ref[IO].of(List.empty[KafkaRecord[_, _]])
      kafkaClient = new TestKafkaClientWithPublishTo(underlying, ref)
      c = new CreateTopicProgram[IO](schemaRegistry,
                                     kafkaClient,
                                     retry,
                                     metadataSubject)
      boot <- Bootstrap.make[IO](c, config)
      _ <- boot.bootstrapAll
      topicCreated <- kafkaClient.describeTopic(metadataSubject.value)
      schemasAdded <- schemaRegistry.getAllSubjects
      messagesPublished <- ref.get
    } yield (topicCreated, schemasAdded, messagesPublished)
  }

  "Bootstrap" must {
    "create the metadata topic" in {
      val config =
        V2MetadataTopicConfig(metadataSubject,
                              createOnStartup = true,
                              createV2TopicsEnabled = true,
                              ContactMethod.create("test@test.com").get,
                              1,
                              1)
      createTestCase(config)
        .map {
          case (topicCreated, schemasAdded, messagesPublished) =>
            topicCreated shouldBe Some(Topic(metadataSubject.value, 1))
            schemasAdded should contain allOf (metadataSubject.value + "-key", metadataSubject.value + "-value")
            messagesPublished.length shouldBe 1
        }
        .unsafeRunSync()
    }

    "not create the metadata topic" in {
      val config =
        V2MetadataTopicConfig(metadataSubject,
                              createOnStartup = false,
                              createV2TopicsEnabled = false,
                              ContactMethod.create("test@test.com").get,
                              1,
                              1)
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

  private final class TestKafkaClientWithPublishTo(
      underlying: KafkaClient[IO],
      publishTo: Ref[IO, List[KafkaRecord[_, _]]]
  ) extends KafkaClient[IO] {

    override def describeTopic(name: TopicName): IO[Option[Topic]] =
      underlying.describeTopic(name)
    override def getTopicNames: IO[List[TopicName]] = underlying.getTopicNames

    override def createTopic(name: TopicName, details: TopicDetails): IO[Unit] =
      underlying.createTopic(name, details)

    override def deleteTopic(name: String): IO[Unit] =
      underlying.deleteTopic(name)

    override def publishMessage[K, V](
        record: KafkaRecord[K, V]
    ): IO[Either[KafkaClient.PublishError, Unit]] =
      publishTo.update(_ :+ record).attemptNarrow[PublishError]
  }

}
