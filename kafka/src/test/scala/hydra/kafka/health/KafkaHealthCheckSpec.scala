package hydra.kafka.health

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit}
import com.github.vonnagy.service.container.health.HealthState
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, FunSpecLike, Matchers}

import scala.concurrent.duration._

@DoNotDiscover
class KafkaHealthCheckSpec extends TestKit(ActorSystem("hydra")) with Matchers with FunSpecLike
  with BeforeAndAfterAll with Eventually with ScalaFutures {

  implicit override val patienceConfig = PatienceConfig(timeout = Span(12, Seconds), interval = Span(5, Millis))
  implicit val config = EmbeddedKafkaConfig(kafkaPort = 8092, zooKeeperPort = 3181)

  override def afterAll = {
    TestKit.shutdownActorSystem(system)
  }


  describe("the Kafka health check") {
    it("publishes an error when it cannot produce to kafka") {
      val act = TestActorRef[KafkaHealthCheckActor](KafkaHealthCheckActor.props("localhost:1111", interval = Some(1.day)))
      whenReady(act.underlyingActor.checkHealth()) { h =>
        h.name shouldBe "Kafka [localhost:1111]"
        h.state shouldBe HealthState.CRITICAL
      }
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
