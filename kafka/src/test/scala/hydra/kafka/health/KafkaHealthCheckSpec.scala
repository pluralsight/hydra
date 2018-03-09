package hydra.kafka.health

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import com.github.vonnagy.service.container.health.HealthState
import hydra.core.protocol.HydraApplicationError
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.duration._

class KafkaHealthCheckSpec extends TestKit(ActorSystem("KafkaHealthCheckSpec"))
  with Matchers
  with FunSpecLike
  with BeforeAndAfterAll
  with Eventually
  with ScalaFutures {

  implicit override val patienceConfig = PatienceConfig(timeout = Span(12, Seconds), interval = Span(5, Millis))
  implicit val config = EmbeddedKafkaConfig(kafkaPort = 8092, zooKeeperPort = 3181,
    customBrokerProperties = Map("auto.create.topics.enable" -> "false"))

  val listener = TestProbe()
  system.eventStream.subscribe(listener.ref, classOf[HydraApplicationError])

  override def afterAll = {
    EmbeddedKafka.stop()
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

  override def beforeAll() = {
    EmbeddedKafka.start()
    EmbeddedKafka.createCustomTopic("_hydra_health_check")
  }

  describe("the Kafka health check") {
    it("publishes an error when it cannot produce to kafka") {
      val act = TestActorRef[KafkaHealthCheckActor](KafkaHealthCheckActor.props("localhost:1111", interval = Some(1.day)))
      whenReady(act.underlyingActor.checkHealth()) { h =>
        h.name shouldBe "Kafka [localhost:1111]"
        h.state shouldBe HealthState.CRITICAL
      }
      listener.expectMsgType[HydraApplicationError]
      system.stop(act)
    }

    it("checks health") {
      val act = TestActorRef[KafkaHealthCheckActor](KafkaHealthCheckActor.props("localhost:8092"))
      whenReady(act.underlyingActor.checkHealth()) { h =>
        h.name shouldBe "Kafka [localhost:8092]"
        h.state shouldBe HealthState.OK
      }

      system.stop(act)
    }
  }
}
