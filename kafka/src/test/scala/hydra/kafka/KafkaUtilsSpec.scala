package hydra.kafka

import com.typesafe.config.ConfigFactory
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.util.KafkaUtils
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.Watcher
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.collection.JavaConverters._

/**
  * Created by alexsilva on 5/17/17.
  */
class KafkaUtilsSpec extends WordSpec
  with BeforeAndAfterAll
  with Matchers
  with Eventually
  with EmbeddedKafka {

  implicit val config = EmbeddedKafkaConfig(kafkaPort = 8092, zooKeeperPort = 3181)

  class MockableZK extends ZkClient("localhost:2181", 5000) {
    override def connect(maxMsToWaitUntilConnected: Long, watcher: Watcher): Unit = {

    }

    override def exists(path: String, watch: Boolean): Boolean = {
      if (path == "/brokers/topics/unknown") false else true
    }


    override def getChildren(path: String, watch: Boolean) = {
      Seq("test-kafka-utils").asJava
    }
  }


  val ku = KafkaUtils("test", () => new MockableZK)

  val cfg = ConfigFactory.parseString(
    """
      |akka {
      |  kafka.producer {
      |    parallelism = 100
      |    close-timeout = 60s
      |    use-dispatcher = test
      |    kafka-clients {
      |       linger.ms = 10
      |    }
      |  }
      |}
      |hydra_kafka {
      |   schema.registry.url = "localhost:808"
      |   kafka.producer {
      |     bootstrap.servers="localhost:8092"
      |     key.serializer = org.apache.kafka.common.serialization.StringSerializer
      |   }
      |   kafka.clients {
      |      test.producer {
      |        value.serializer = org.apache.kafka.common.serialization.StringSerializer
      |      }
      |      test1.producer {
      |        value.serializer = org.apache.kafka.common.serialization.Tester
      |      }
      |   }
      |}
      |
      """.stripMargin)

  "Kafka Utils" should {
    "return false for a topic that doesn't exist" in {
      val exists = ku.topicExists("unknown").get
      assert(!exists)
    }

    "uses the zkString in the config" in {
      KafkaUtils().zkString shouldBe KafkaConfigSupport.zkString
    }

    "return true for a topic that exists" in {
      assert(ku.topicExists("test-kafka-utils").isSuccess)
    }

    "return a list of topics" in {
      ku.topicNames().get.indexOf("test-kafka-utils") should be > -1
    }

    "loads default consumer" in {
      val d = KafkaUtils.consumerForClientId("string")
      val props = Map(
        "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        "auto.offset.reset" -> "latest",
        "group.id" -> "hydra",
        "bootstrap.servers" -> "localhost:8092",
        "enable.auto.commit" -> "false",
        "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        "zookeeper.connect" -> "localhost:3181",
        "client.id" -> "string",
        "metadata.fetch.timeout.ms" -> "100000")

      d.get.properties shouldBe props
    }

    "has settings for consumers by client id" in {
      val d = KafkaUtils.loadConsumerSettings("avro", "hydrag")
      val props = Map(
        "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        "auto.offset.reset" -> "latest",
        "group.id" -> "hydrag",
        "bootstrap.servers" -> "localhost:8092",
        "enable.auto.commit" -> "false",
        "value.deserializer" -> "io.confluent.kafka.serializers.KafkaAvroDeserializer",
        "zookeeper.connect" -> "localhost:3181",
        "client.id" -> "avro",
        "metadata.fetch.timeout.ms" -> "100000",
        "schema.registry.url" -> "mock")

      d.properties shouldBe props
    }

    "create ProducerSettings from config" in {

      val settings = KafkaUtils.producerSettings("test", cfg)

      settings.properties shouldBe Map(
        "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        "bootstrap.servers" -> "localhost:8092",
        "client.id" -> "test",
        "linger.ms" -> "10")
    }

    "retrieve all clients from a config" in {
      val clients = KafkaUtils.producerSettings(cfg)
      clients.keys should contain allOf("test", "test1")
      clients("test1").properties shouldBe Map(
        "value.serializer" -> "org.apache.kafka.common.serialization.Tester",
        "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        "bootstrap.servers" -> "localhost:8092",
        "client.id" -> "test1",
        "linger.ms" -> "10")
    }
  }
}

