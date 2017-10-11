package hydra.kafka.health

import akka.actor.{ActorSystem, PoisonPill}
import akka.testkit.{TestActorRef, TestKit}
import com.github.vonnagy.service.container.health.HealthState
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.duration._

class KafkaHealthCheckSpec extends TestKit(ActorSystem("hydra")) with Matchers with FunSpecLike
  with BeforeAndAfterAll with EmbeddedKafka with Eventually with ScalaFutures {

  implicit override val patienceConfig = PatienceConfig(timeout = Span(12, Seconds), interval = Span(5, Millis))
  implicit val config = EmbeddedKafkaConfig(kafkaPort = 8092, zooKeeperPort = 3181)

  override def afterAll = {
    EmbeddedKafka.stop()
    TestKit.shutdownActorSystem(system)
  }

  override def beforeAll() = EmbeddedKafka.start()

  describe("the Kafka health check") {
    it("publishes an error when it cannot produce to kafka") {
      val act = TestActorRef[KafkaHealthCheckActor](KafkaHealthCheckActor.props("localhost:1111", interval = Some(1.day)))
      whenReady(act.underlyingActor.checkHealth()) { h =>
        h.name shouldBe "Kafka [localhost:1111]"
        h.state shouldBe HealthState.CRITICAL
      }
      act ! PoisonPill
    }

    it("checks health") {
      val act = TestActorRef[KafkaHealthCheckActor](KafkaHealthCheckActor.props("localhost:8092"))
      whenReady(act.underlyingActor.checkHealth()) { h =>
        h.name shouldBe "Kafka [localhost:8092]"
        h.state shouldBe HealthState.OK
      }

      act ! PoisonPill
    }
  }
}
