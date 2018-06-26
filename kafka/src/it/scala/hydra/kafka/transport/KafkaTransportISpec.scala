package hydra.kafka.transport

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.{Config, ConfigFactory}
import hydra.common.config.ConfigSupport
import hydra.kafka.producer.{DeleteTombstoneRecord, JsonRecord, KafkaRecordMetadata, StringRecord}
import kamon.MetricReporter
import kamon.metric.PeriodSnapshot
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}


class KafkaTransportISpec extends TestKit(ActorSystem("hydra"))
  with Matchers
  with FunSpecLike
  with Eventually
  with ImplicitSender
  with BeforeAndAfterAll
  with ConfigSupport {

  lazy val transport = system.actorOf(KafkaTransport.props(rootConfig), "kafka")

  val reporter = new MetricReporter {

    var snapshot: PeriodSnapshot = _

    override def reportPeriodSnapshot(snapshot: PeriodSnapshot): Unit = {
      this.snapshot = snapshot
    }

    override def start(): Unit = {}

    override def stop(): Unit = {}

    override def reconfigure(config: Config): Unit = {}
  }

  override def beforeAll() = {
    super.beforeAll()
    EmbeddedKafka.start()
    EmbeddedKafka.createCustomTopic("transport_test")
  }

  override def afterAll() = {
    super.afterAll()
    system.stop(transport)
    EmbeddedKafka.stop()
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }


  val metadata = KafkaRecordMetadata(1, 1L, "test.topic", 2, 3)

  describe("When using the KafkaTransport") {

    it("should increment message rate counters") {
      transport ! metadata
      eventually {
        val snapshot = reporter.snapshot
        snapshot.metrics.counters
          .filter(_.name == KafkaTransport.counterMetricName)
          .head
          .value shouldBe 1
      }
    }
  }

}
