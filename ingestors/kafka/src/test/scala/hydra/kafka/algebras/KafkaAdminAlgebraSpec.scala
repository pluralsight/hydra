package hydra.kafka.algebras

import akka.actor.ActorSystem
import cats.effect.{ContextShift, IO}
import cats.implicits._
import hydra.kafka.util.KafkaUtils.TopicDetails
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContext

final class KafkaAdminAlgebraSpec
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
    live <- KafkaAdminAlgebra
      .live[IO](s"localhost:$port")
    test <- KafkaAdminAlgebra.test[IO]
  } yield {
    runTests(live)
    runTests(test, isTest = true)
  }).unsafeRunSync()

  private def runTests(kafkaClient: KafkaAdminAlgebra[IO], isTest: Boolean = false): Unit = {
    (if (isTest) "KafkaAdmin#test" else "KafkaAdmin#live") must {
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
}
