package hydra.kafka.streams

import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpecLike, Matchers}

class ProducersSpec extends Matchers with FlatSpecLike {

  "The Kafka Producers Helper object" should "create ProducerSettings from config" in {
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
        |   kafka.producer.bootstrap.servers="localhost:8092"
        |   kafka.clients {
        |      test {
        |        key.serializer = org.apache.kafka.common.serialization.StringSerializer
        |        value.serializer = org.apache.kafka.common.serialization.StringSerializer
        |      }
        |   }
        |}
        |
      """.stripMargin)
    val settings = Producers.producerSettings("test", cfg)
    settings.properties shouldBe Map(
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "bootstrap.servers" -> "localhost:8092",
      "client.id" -> "test",
      "linger.ms" -> "10")
  }

}
