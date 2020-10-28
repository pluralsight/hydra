package hydra.kafka.transport

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import hydra.core.transport.AckStrategy
import hydra.kafka.producer.KafkaRecordMetadata
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.BeforeAndAfterAll
import spray.json.DefaultJsonProtocol

/**
  * Created by alexsilva on 12/5/16.
  */
class KafkaMetricsSpec
    extends TestKit(ActorSystem("hydra"))
    with Matchers
    with AnyFunSpecLike
    with BeforeAndAfterAll
    with DefaultJsonProtocol {

  import KafkaRecordMetadata._

  implicit val config = EmbeddedKafkaConfig(
    kafkaPort = 8012,
    zooKeeperPort = 3111,
    customBrokerProperties = Map(
      "auto.create.topics.enable" -> "false",
      "offsets.topic.replication.factor" -> "1"
    )
  )

  override def afterAll() = {
    super.afterAll()
    EmbeddedKafka.stop()
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  override def beforeAll() = {
    super.beforeAll()
    EmbeddedKafka.start()
    EmbeddedKafka.createCustomTopic("metrics_topic")
  }

  describe("When using the KafkaMetrics object") {

    it("uses the NoOpMetrics") {
      KafkaMetrics(ConfigFactory.empty()) shouldBe NoOpMetrics
      KafkaMetrics(
        ConfigFactory.parseString("transports.kafka.metrics.enabled=false")
      ) shouldBe NoOpMetrics
    }

    it("uses the PublishMetrics") {
      import spray.json._
      val cfg = ConfigFactory.parseString(s"""
           | transports.kafka.metrics.topic = metrics_topic
           | transports.kafka.metrics.enabled=true""".stripMargin)
      val pm = KafkaMetrics(cfg)
      pm shouldBe a[PublishMetrics]
      val kmd = KafkaRecordMetadata(1, 1, "topic", 1, 1, AckStrategy.NoAck)
      pm.saveMetrics(kmd)
      EmbeddedKafka
        .consumeFirstStringMessageFrom("metrics_topic")
        .parseJson shouldBe kmd.toJson

    }
  }
}
