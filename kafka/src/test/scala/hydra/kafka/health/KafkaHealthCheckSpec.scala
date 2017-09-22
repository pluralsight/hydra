package hydra.kafka.health

import akka.actor.{ActorSystem, PoisonPill}
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import com.github.vonnagy.service.container.health.{CheckHealth, HealthInfo, HealthState}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.duration._

class KafkaHealthCheckSpec extends TestKit(ActorSystem("hydra")) with Matchers with FunSpecLike
  with BeforeAndAfterAll with EmbeddedKafka with Eventually {

  implicit override val patienceConfig = PatienceConfig(timeout = Span(12, Seconds), interval = Span(5, Millis))

  override def afterAll = {
    TestKit.shutdownActorSystem(system)
  }

  val listener = TestProbe()

  system.eventStream.subscribe(listener.ref, classOf[HealthInfo])

  describe("the Kafka health check") {
    it("publishes an error when it cannot produce to kafka") {
      val act = system.actorOf(KafkaHealthCheckActor.props(Some(1.second)))
      act ! CheckHealth
      listener.expectMsgPF() {
        case h: HealthInfo =>
          h.name shouldBe "Kafka [localhost:8092]"
          h.state shouldBe HealthState.CRITICAL
      }
      act ! PoisonPill
    }

    it("checks health") {
      implicit val config = EmbeddedKafkaConfig(kafkaPort = 8092, zooKeeperPort = 3181)
      val act = TestActorRef[KafkaHealthCheckActor](KafkaHealthCheckActor.props(None))
      withRunningKafka {
        EmbeddedKafka.createCustomTopic("__hydra_health_check")
        EmbeddedKafka.publishStringMessageToKafka("__hydra_health_check", "test")
        //change the underlying state on purpose to see if we get the "OK" after the health check completes
        act.underlyingActor.currentHealth = HealthInfo("test", details = "", state = HealthState.CRITICAL)
        act ! CheckHealth
        listener.expectMsgPF() {
          case h: HealthInfo =>
            h.name shouldBe "Kafka [localhost:8092]"
            h.state shouldBe HealthState.OK
        }
        //make sure it doesn't publish the same event twice
        act ! CheckHealth
        listener.expectNoMsg()
      }
      act ! PoisonPill
    }
  }
}
