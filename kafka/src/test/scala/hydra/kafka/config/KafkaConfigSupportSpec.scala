/*
 * Copyright (C) 2017 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hydra.kafka.config

import com.typesafe.config.ConfigFactory
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.scalatest.{FunSpecLike, Matchers}

import scala.collection.JavaConverters._

/**
 * Created by alexsilva on 9/7/16.
 */
class KafkaConfigSupportSpec extends Matchers with FunSpecLike with KafkaConfigSupport {

  describe("When using the provided reference.conf") {
    it("Should have two message types") {
      val m = applicationConfig.getObject("kafka.formats")
      m.entrySet().size() shouldBe 2
      m.asScala.map(_._1) should contain allOf ("string", "avro")
    }

    it("Should return an empty config for an unknown type") {
      consumerConfig("unknown") shouldBe new java.util.Properties()
    }
  }

  describe("When testing producer configs") {

    it("returns a producer with default values.") {

      val producerCfg = ConfigFactory.parseString(
        """
          |kafka {
          |    formats {
          |      avro {
          |        key.serializer = "org.apache.kafka.common.serialization.StringSerializer"
          |        key.deserializer = "org.apache.kafka.common.serialization.StringDeserializer"
          |        value.serializer = "io.confluent.kafka.serializers.KafkaAvroSerializer"
          |        value.deserializer = "io.confluent.kafka.serializers.KafkaAvroDeserializer"
          |        client.id = "hydra.avro"
          |      }
          |     }
          |  producer {
          |    bootstrap.servers = "kafka:6667"
          |    schema.registry.url = "schema:8081"
          |  }
          |}
        """.stripMargin
      )

      val m = KafkaConfigSupport.loadProducerFormats(producerCfg)("avro")
      m.getString("schema.registry.url") shouldBe "schema:8081"
      m.getString("bootstrap.servers") shouldBe "kafka:6667"
      m.getString("key.deserializer") shouldBe "org.apache.kafka.common.serialization.StringDeserializer"
      m.getString("key.serializer") shouldBe "org.apache.kafka.common.serialization.StringSerializer"
      m.getString("client.id") shouldBe "hydra.avro"
      m.hasPath("zookeeper.connect") shouldBe false
      m.getString("value.serializer") shouldBe "io.confluent.kafka.serializers.KafkaAvroSerializer"
      m.getString("value.deserializer") shouldBe "io.confluent.kafka.serializers.KafkaAvroDeserializer"
    }

    it("Should return a producer config with overridden values.") {

      val noProducerCfg = ConfigFactory.parseString(
        """
          |kafka {
          |    formats {
          |      avro {
          |        key.serializer = "org.apache.kafka.common.serialization.StringSerializer"
          |        key.deserializer = "org.apache.kafka.common.serialization.StringDeserializer"
          |        value.serializer = "io.confluent.kafka.serializers.KafkaAvroSerializer"
          |        value.deserializer = "io.confluent.kafka.serializers.KafkaAvroDeserializer"
          |        client.id = "hydra.avro"
          |      }
          |     }
          |  health_check.interval = 10s
          |  producer {
          |    bootstrap.servers = "overridden:6667"
          |     schema.registry.url = "schema:8081"
          |  }
          |  consumer {
          |    zookeeper.connect = "172.16.100.47:2181,172.16.100.48:2181,172.16.100.49:2181,172.16.100.51:2181"
          |     schema.registry.url = "schema:8081"
          |  }
          |}
        """.stripMargin
      )

      val m = KafkaConfigSupport.loadProducerFormats(noProducerCfg)("avro")
      m.entrySet().size shouldBe 7
      m.getString("schema.registry.url") shouldBe "schema:8081"
      m.getString("bootstrap.servers") shouldBe "overridden:6667"
      m.getString("key.deserializer") shouldBe "org.apache.kafka.common.serialization.StringDeserializer"
      m.getString("key.serializer") shouldBe "org.apache.kafka.common.serialization.StringSerializer"
      m.getString("client.id") shouldBe "hydra.avro"
      m.hasPath("zookeeper.connect") shouldBe false
      m.getString("value.serializer") shouldBe "io.confluent.kafka.serializers.KafkaAvroSerializer"
      m.getString("value.deserializer") shouldBe "io.confluent.kafka.serializers.KafkaAvroDeserializer"
    }

    it("Should return a producer config with overridden format values.") {

      val noProducerCfg = ConfigFactory.parseString(
        """
          |kafka {
          |    formats {
          |      avro {
          |        key.serializer = "org.apache.kafka.common.serialization.StringSerializer"
          |        key.deserializer = "org.apache.kafka.common.serialization.StringDeserializer"
          |        value.serializer = "io.confluent.kafka.serializers.KafkaAvroSerializer"
          |        value.deserializer = "io.confluent.kafka.serializers.KafkaAvroDeserializer"
          |        bootstrap.servers = "test:6667"
          |      }
          |     }
          |  producer {
          |    bootstrap.servers = "kafka:6667"
          |    client.id = "hydra.test"
          |    schema.registry.url = "schema:8081"
          |  }
          |  consumer {
          |    zookeeper.connect = "172.16.100.47:2181,172.16.100.48:2181,172.16.100.49:2181,172.16.100.51:2181"
          |    bootstrap.servers = "kafka:6667"
          |    schema.registry.url = "schema:8081"
          |  }
          |}
        """.stripMargin
      )

      val m = KafkaConfigSupport.loadProducerFormats(noProducerCfg)("avro")
      m.entrySet().size shouldBe 7
      m.getString("schema.registry.url") shouldBe "schema:8081"
      m.getString("bootstrap.servers") shouldBe "test:6667"
      m.getString("key.deserializer") shouldBe "org.apache.kafka.common.serialization.StringDeserializer"
      m.getString("key.serializer") shouldBe "org.apache.kafka.common.serialization.StringSerializer"
      m.getString("client.id") shouldBe "hydra.test"
      m.hasPath("zookeeper.connect") shouldBe false
      m.getString("value.serializer") shouldBe "io.confluent.kafka.serializers.KafkaAvroSerializer"
      m.getString("value.deserializer") shouldBe "io.confluent.kafka.serializers.KafkaAvroDeserializer"
    }

    it("Should return a consumer config with default values") {
      val m = consumerConfig("avro")
      m should contain key ("schema.registry.url")
      m should contain key ("value.deserializer")
      m should contain key ("key.serializer")
      m should contain key ("group.id")
      m.get("value.serializer") shouldBe classOf[KafkaAvroSerializer].getName
      m should contain key ("zookeeper.connect")
      m should contain key ("value.deserializer")
      m.get("value.deserializer") shouldBe classOf[KafkaAvroDeserializer].getName
    }

    it("Should return a consumer config with overridden values") {
      val f = scala.util.Random.nextFloat().toString
      val m = consumerConfig("avro",
        ConfigFactory.parseMap(Map("bootstrap.servers" -> f, "group.id" -> f).asJava))
      m.get("value.serializer") shouldBe classOf[KafkaAvroSerializer].getName
      m.get("value.deserializer") shouldBe classOf[KafkaAvroDeserializer].getName
      m.get("bootstrap.servers") shouldBe f
      m.get("group.id") shouldBe f
    }

    it("Should return the default string config for unknown topics") {
      val c = topicConsumerConfigs("unknown")
      c shouldBe kafkaConsumerFormats("string")
    }
  }
}