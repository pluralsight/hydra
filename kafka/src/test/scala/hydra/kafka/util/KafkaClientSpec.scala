package hydra.kafka.util

import cats.effect.{ContextShift, IO}
import hydra.kafka.util.KafkaUtils.TopicDetails
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import cats.implicits._

import scala.concurrent.ExecutionContext

class KafkaClientSpec extends WordSpec with Matchers with BeforeAndAfterAll with EmbeddedKafka {

  private val port = 8023
  implicit private val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = port, zooKeeperPort = 3027)
  implicit private val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    EmbeddedKafka.stop()
  }

  (for {
    live <- KafkaClient.live[IO](s"localhost:$port")
    test <- KafkaClient.test[IO]
  } yield {
    runTests(live, isTest = false)
    runTests(test, isTest = true)
  }).unsafeRunSync()

  def runTests(kafkaClient: KafkaClient[IO], isTest: Boolean): Unit = {
    (if (isTest) "KafkaClient#test" else "KafkaClient#live") must {
      "create a topic" in {
        val topicName = "Topic1"
        val topicDetails = TopicDetails(3, 1.toShort)
        (kafkaClient.createTopic(topicName, topicDetails) *> kafkaClient.describeTopic(topicName).map {
          case Some(topic) =>
            topic.name shouldBe topicName
            topic.numberPartitions shouldBe topicDetails.numPartitions
          case None => fail("Found None when a Topic was Expected")
        }).unsafeRunSync()
      }

      "list all topics" in {
        kafkaClient.getTopicNames.unsafeRunSync() shouldBe List("Topic1")
      }

    }
  }

}
