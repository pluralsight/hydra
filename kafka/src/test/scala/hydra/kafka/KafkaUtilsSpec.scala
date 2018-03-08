package hydra.kafka

import com.typesafe.config.ConfigFactory
import hydra.kafka.util.KafkaUtils
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}


/**
  * Created by alexsilva on 5/17/17.
  */
class KafkaUtilsSpec extends WordSpec
  with BeforeAndAfterAll
  with Matchers
  with Eventually {

  implicit val config = EmbeddedKafkaConfig(kafkaPort = 8092, zooKeeperPort = 3181)

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
      |hydra {
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

  override def beforeAll() = {
    super.beforeAll()
    EmbeddedKafka.start()
    EmbeddedKafka.createCustomTopic("test-kafka-utils")
  }

  override def afterAll() = {
    super.afterAll()
    KafkaUtils.zkUtils.foreach(_.close())
    EmbeddedKafka.stop()
  }

  "Kafka Utils" should {
    "return false for a topic that doesn't exist" in {
      val exists = KafkaUtils.topicExists("test_123123").get //should be false
      assert(!exists)
    }

    "return true for a topic that exists" in {
      assert(KafkaUtils.topicExists("test-kafka-utils").isSuccess)
    }

    "return a list of topics" in {
      KafkaUtils.topicNames().get.indexOf("test-kafka-utils") should be > -1
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

