package hydra.kafka.health

import akka.actor.{ActorSystem, Props}
import akka.testkit.{TestKit, TestProbe}
import com.github.vonnagy.service.container.health.{CheckHealth, HealthInfo, HealthState}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.duration._

class ZookeeperHealthCheckSpec extends TestKit(ActorSystem("hydra")) with Matchers with FunSpecLike
  with BeforeAndAfterAll with EmbeddedKafka with Eventually {

  implicit val config = EmbeddedKafkaConfig(kafkaPort = 8092, zooKeeperPort = 3181)

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
  }

  val listener = TestProbe()

  system.eventStream.subscribe(listener.ref, classOf[HealthInfo])

  val act = system.actorOf(Props[ZookeeperHealthCheckActor])

  describe("the ZK health check") {
    it("publishes an error when it cannot talk to Zookeeper") {
      act ! CheckHealth
      listener.expectMsgPF(10.seconds) {
        case h: HealthInfo =>
          h.name shouldBe "Zookeeper [localhost:3181]"
          h.state shouldBe HealthState.CRITICAL
      }
    }
  }

  it("reports critical if no topics found in kafka") {
    withRunningKafka {
      act ! CheckHealth
      listener.expectMsgPF() {
        case h: HealthInfo =>
          h.state shouldBe HealthState.CRITICAL
      }
    }
  }

  it("reports OK") {
    withRunningKafka {
      EmbeddedKafka.createCustomTopic("test")
      EmbeddedKafka.publishStringMessageToKafka("test", "test")
      act ! CheckHealth
      listener.expectMsgPF() {
        case h: HealthInfo =>
          h.name shouldBe "Zookeeper [localhost:3181]"
          h.state shouldBe HealthState.OK
      }
      //make sure it doesn't publish the same event twice
      act ! CheckHealth
      listener.expectNoMsg()
    }

  }
}
