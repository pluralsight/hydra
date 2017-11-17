package hydra.kafka.transport

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import hydra.kafka.producer.KafkaRecordMetadata
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, FunSpecLike, Matchers}
import spray.json.DefaultJsonProtocol

/**
  * Created by alexsilva on 12/5/16.
  */
@DoNotDiscover
class KafkaMetricsSpec extends TestKit(ActorSystem("hydra")) with Matchers with FunSpecLike
  with BeforeAndAfterAll with DefaultJsonProtocol {

  private implicit val mdFormat = jsonFormat5(KafkaRecordMetadata.apply)

  implicit val config = EmbeddedKafkaConfig(kafkaPort = 8092, zooKeeperPort = 3181,
    customBrokerProperties = Map("auto.create.topics.enable" -> "false"))

  override def afterAll() = {
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  override def beforeAll() = {
    super.beforeAll()
    EmbeddedKafka.createCustomTopic("metrics_topic")
  }

  describe("When using the KafkaMetrics object") {

    it("uses the NoOpMetrics") {
      KafkaMetrics(ConfigFactory.empty()) shouldBe NoOpMetrics
      KafkaMetrics(ConfigFactory.parseString("transports.kafka.metrics.enabled=false")) shouldBe NoOpMetrics
    }

    it("uses the PublishMetrics") {
      import spray.json._
      val cfg = ConfigFactory.parseString(
        s"""
           | transports.kafka.metrics.topic = metrics_topic
           | transports.kafka.metrics.enabled=true""".stripMargin)
      val pm = KafkaMetrics(cfg)
      pm shouldBe a[PublishMetrics]
      val kmd = KafkaRecordMetadata(1, 1, "topic", 1, 1)
      pm.saveMetrics(kmd)
      EmbeddedKafka.consumeFirstStringMessageFrom("metrics_topic").parseJson shouldBe kmd.toJson

    }
  }
}
