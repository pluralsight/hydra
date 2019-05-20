package hydra.kafka.ingestors

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import hydra.common.config.ConfigSupport
import hydra.kafka.ingestors.KafkaTopicActor.{GetTopicRequest, GetTopicResponse}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._

class KafkaTopicsActorSpec
  extends TestKit(ActorSystem("kafka-topics-spec", config = ConfigFactory.parseString("akka.actor.provider=cluster")))
    with Matchers
    with FlatSpecLike
    with ImplicitSender
    with ConfigSupport
    with Eventually
    with EmbeddedKafka
    with BeforeAndAfterAll {

  val config = ConfigFactory.parseString(
    """
      |  bootstrap.servers = "localhost:6001"
      |  zookeeper = "localhost:6000"
    """.stripMargin)


  implicit val patience = PatienceConfig(timeout = 10 seconds, interval = 1 second)

  override def afterAll = {
    EmbeddedKafka.stop()
    TestKit.shutdownActorSystem(system)
  }

  override def beforeAll = EmbeddedKafka.start()

  "A KafkaTopicsActor" should "return topics that exist" in {
    EmbeddedKafka.createCustomTopic("topic-actor")
    val actor = system.actorOf(KafkaTopicActor.props(config))
    actor ! GetTopicRequest("topic-actor")
    expectMsgPF() {
      case GetTopicResponse(t, _, e) =>
        t shouldBe "topic-actor"
        e shouldBe true
    }
  }

  it should "not return topics that doesn't exist" in {
    val actor = system.actorOf(KafkaTopicActor.props(config))
    actor ! GetTopicRequest("test-topic")
    expectMsgPF() {
      case GetTopicResponse(t, _, e) =>
        t shouldBe "test-topic"
        e shouldBe false
    }
  }

  it should "update its local cache" in {
    val actor = system.actorOf(KafkaTopicActor.props(config))
    actor ! GetTopicRequest("new-topic")
    expectMsgPF() {
      case GetTopicResponse(t, _, e) =>
        t shouldBe "new-topic"
        e shouldBe false
    }
    EmbeddedKafka.createCustomTopic("new-topic")
    eventually {
      actor ! GetTopicRequest("new-topic")
      expectMsgPF() {
        case GetTopicResponse(t, _, e) =>
          t shouldBe "new-topic"
          e shouldBe true
      }
    }
  }
}
