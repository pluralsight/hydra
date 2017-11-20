package hydra.kafka.health

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit}
import com.github.vonnagy.service.container.health.HealthState
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.duration._

class ZookeeperHealthCheckSpec extends TestKit(ActorSystem("hydra")) with Matchers with FunSpecLike
  with BeforeAndAfterAll with ScalaFutures {
  implicit val config = EmbeddedKafkaConfig(kafkaPort = 8092, zooKeeperPort = 3181,
    customBrokerProperties = Map("auto.create.topics.enable" -> "false"))

  override def afterAll() = {
    super.afterAll()
    EmbeddedKafka.stop()
    TestKit.shutdownActorSystem(system)
  }

  override def beforeAll()={
    EmbeddedKafka.start()
  }

  describe("the ZK health check") {
    it("publishes an error when it cannot talk to Zookeeper") {
      val act = TestActorRef[ZookeeperHealthCheckActor](ZookeeperHealthCheckActor.props("localhost:1234", 1.day))
      whenReady(act.underlyingActor.checkHealth()) { h =>
        h.name shouldBe "Zookeeper [localhost:1234]"
        h.state shouldBe HealthState.CRITICAL
      }
      system.stop(act)
    }


    it("reports OK") {
      EmbeddedKafka.createCustomTopic("test")
      EmbeddedKafka.publishStringMessageToKafka("test", "test")
      val act = TestActorRef[ZookeeperHealthCheckActor](ZookeeperHealthCheckActor.props("localhost:3181", 1.day))
      whenReady(act.underlyingActor.checkHealth()) { h =>
        h.name shouldBe "Zookeeper [localhost:3181]"
        h.state shouldBe HealthState.OK
      }
      system.stop(act)
    }
  }
}