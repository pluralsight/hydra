package hydra.kafka.transport

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.{Config, ConfigFactory}
import hydra.common.config.ConfigSupport
import hydra.core.transport.Transport.Deliver
import hydra.core.transport.{RecordMetadata, TransportCallback}
import hydra.kafka.producer.{DeleteTombstoneRecord, JsonRecord, KafkaRecordMetadata, StringRecord}
import hydra.kafka.transport.KafkaProducerProxy.ProducerInitializationError
import hydra.kafka.transport.KafkaTransport.RecordProduceError
import kamon.MetricReporter
import kamon.metric.PeriodSnapshot
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.SerializationException
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.duration._

class KafkaTransportISpec extends TestKit(ActorSystem("hydra"))
  with Matchers
  with FunSpecLike
  with Eventually
  with ImplicitSender
  with BeforeAndAfterAll
  with ConfigSupport {

  lazy val transport = system.actorOf(KafkaTransport.props(rootConfig), "kafka")

  implicit val config = EmbeddedKafkaConfig(kafkaPort = 8092, zooKeeperPort = 3181,
    customBrokerProperties = Map("auto.create.topics.enable" -> "false"))

  val reporter = new MetricReporter {

    var snapshot: PeriodSnapshot = _

    override def reportPeriodSnapshot(snapshot: PeriodSnapshot): Unit = {
      this.snapshot = snapshot
    }

    override def start(): Unit = {}

    override def stop(): Unit = {}

    override def reconfigure(config: Config): Unit = {}
  }

  val metadata = KafkaRecordMetadata(1, 1L, "test.topic", 2, 3)

  describe("When using the KafkaTransport") {

    it("should increment message rate counters") {
      transport ! KafkaRecordMetadata
      eventually {
        reporter.snapshot.metrics.counters
          .filter(_.name == KafkaTransport.counterMetricName)

      }
    }
  }

}
