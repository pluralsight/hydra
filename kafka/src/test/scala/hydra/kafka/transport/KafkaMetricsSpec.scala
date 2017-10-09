package hydra.kafka.transport

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.config.ConfigFactory
import hydra.core.transport.DeliveryStrategy
import hydra.kafka.producer.KafkaRecordMetadata
import hydra.kafka.transport.KafkaProducerProxy.ProduceToKafka
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * Created by alexsilva on 12/5/16.
  */
class KafkaMetricsSpec extends TestKit(ActorSystem("hydra")) with Matchers with FunSpecLike with BeforeAndAfterAll {

  override def afterAll() = TestKit.shutdownActorSystem(system, verifySystemShutdown = true)

  val probe = TestProbe()

  describe("When using the KafkaMetrics object") {

    it("uses the NoOpMetrics") {
      KafkaMetrics(ConfigFactory.empty()) shouldBe NoOpMetrics
      KafkaMetrics(ConfigFactory.parseString("producers.kafka.metrics.enabled=false")) shouldBe NoOpMetrics
    }

    it("uses the PublishMetrics") {
      val cfg = ConfigFactory.parseString(
        s"""
           |producers.kafka.metrics.enabled=true
           |producers.kafka.metrics.actor_path="${probe.ref.path}"""".stripMargin)
      val pm = KafkaMetrics(cfg)
      pm shouldBe a[PublishMetrics]
      pm.saveMetrics(KafkaRecordMetadata(1, 1, "topic", 1, 1, DeliveryStrategy.AtLeastOnce))
      probe.expectMsgType[ProduceToKafka[String, JsonNode]]
    }
  }
}
