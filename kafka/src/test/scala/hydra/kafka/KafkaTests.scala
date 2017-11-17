package hydra.kafka

import hydra.kafka.consumer.KafkaConsumerProxySpec
import hydra.kafka.health.{KafkaHealthCheckSpec, ZookeeperHealthCheckSpec}
import hydra.kafka.transport.{KafkaMetricsSpec, KafkaProducerProxySpec, KafkaTransportSpec}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{BeforeAndAfterAll, Suites}

class KafkaTests extends Suites(new ZookeeperHealthCheckSpec, new KafkaHealthCheckSpec, new KafkaMetricsSpec,
  new KafkaConsumerProxySpec, new KafkaProducerProxySpec, new KafkaTransportSpec, new KafkaUtilsSpec)
  with BeforeAndAfterAll {
  implicit val config = EmbeddedKafkaConfig(kafkaPort = 8092, zooKeeperPort = 3181,
    customBrokerProperties = Map("auto.create.topics.enable" -> "false"))
  
  override def beforeAll(): Unit = EmbeddedKafka.start()

  override def afterAll() = EmbeddedKafka.stop()
}
