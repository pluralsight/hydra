package hydra.kafka.util

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import cats.effect.{ContextShift, IO}
import cats.implicits._
import hydra.core.protocol.{Ingest, IngestorCompleted, IngestorError, IngestorStatus, IngestorTimeout, RequestPublished}
import hydra.core.transport.AckStrategy
import hydra.kafka.producer.StringRecord
import hydra.kafka.util.KafkaClient.PublishError
import hydra.kafka.util.KafkaUtils.TopicDetails
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

final class KafkaClientSpec
    extends AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with EmbeddedKafka {

  private val port = 8023

  implicit private val kafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort = port, zooKeeperPort = 3027)

  implicit private val contextShift: ContextShift[IO] =
    IO.contextShift(ExecutionContext.global)

  implicit private val system: ActorSystem = ActorSystem(
    "kafka-client-spec-system"
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    EmbeddedKafka.stop()
  }

  (for {
    live <- KafkaClient
      .live[IO](s"localhost:$port", system.actorSelection(TestProbe().ref.path))
    test <- KafkaClient.test[IO]
  } yield {
    runTests(live, isTest = false)
    runTests(test, isTest = true)
  }).unsafeRunSync()

  runLiveOnlyTests()

  private def runTests(kafkaClient: KafkaClient[IO], isTest: Boolean): Unit = {
    (if (isTest) "KafkaClient#test" else "KafkaClient#live") must {
      "create a topic" in {
        val topicName = "Topic1"
        val topicDetails = TopicDetails(3, 1.toShort)
        (kafkaClient.createTopic(topicName, topicDetails) *> kafkaClient
          .describeTopic(topicName)
          .map {
            case Some(topic) =>
              topic.name shouldBe topicName
              topic.numberPartitions shouldBe topicDetails.numPartitions
            case None => fail("Found None when a Topic was Expected")
          }).unsafeRunSync()
      }

      "list all topics" in {
        kafkaClient.getTopicNames.unsafeRunSync() shouldBe List("Topic1")
      }

      "delete a topic" in {
        val topicToDelete = "topic_to_delete"
        (for {
          _ <- kafkaClient.createTopic(topicToDelete, TopicDetails(1, 1))
          _ <- kafkaClient.deleteTopic(topicToDelete)
          maybeTopic <- kafkaClient.describeTopic(topicToDelete)
        } yield maybeTopic should not be defined).unsafeRunSync()
      }

    }
  }

  private def runLiveOnlyTests(): Unit = {
    val probe = TestProbe()
    KafkaClient
      .live[IO](s"test", system.actorSelection(probe.ref.path))
      .map { kafkaClient =>
        def testCase(
            ingestorReply: IngestorStatus,
            expectedResult: Either[PublishError, Unit]
        ): Unit = {
          val record = StringRecord(
            "some_test_topic",
            "key",
            "payload",
            AckStrategy.Replicated
          )
          (for {
            f <- kafkaClient.publishMessage(record).start
            _ <- IO(probe.expectMsg(Ingest(record, AckStrategy.Replicated)))
            _ <- IO(probe.reply(ingestorReply))
            result <- f.join
          } yield result shouldBe expectedResult).unsafeRunSync()
        }
        "KafkaClient#live" must {
          "send ingest request to ingestActor" in {
            testCase(IngestorCompleted, Right(()))
          }

          "handle ingestor timeout" in {
            testCase(IngestorTimeout, Left(PublishError.Timeout))
          }

          "handle unknown responses" in {
            testCase(
              RequestPublished,
              Left(PublishError.UnexpectedResponse(RequestPublished))
            )
          }

          "handle ingestor error" in {
            val exception = new Exception("Error")
            testCase(
              IngestorError(exception),
              Left(PublishError.Failed(exception))
            )
          }
        }
      }
      .unsafeRunSync()
  }

}
