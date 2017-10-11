package hydra.kafka.transport

import akka.actor.{ActorSystem, PoisonPill}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import hydra.core.protocol.{RecordNotProduced, RecordProduced}
import hydra.core.transport.DeliveryStrategy
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.producer.{JsonRecord, KafkaRecordMetadata, StringRecord}
import hydra.kafka.transport.KafkaProducerProxy.{ProduceOnly, ProducerInitializationError}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpecLike, Matchers}

import scala.concurrent.duration._

/**
  * Created by alexsilva on 12/5/16.
  */
class KafkaProducerProxySpec extends TestKit(ActorSystem("hydra")) with Matchers with FunSpecLike with ImplicitSender
  with BeforeAndAfterAll with BeforeAndAfterEach with KafkaConfigSupport with EmbeddedKafka {

  implicit val config = EmbeddedKafkaConfig(kafkaPort = 8092, zooKeeperPort = 3181,
    customBrokerProperties = Map("auto.create.topics.enable" -> "false"))

  val parent = TestProbe()

  val kafkaActor = parent.childActorOf(KafkaProducerProxy.props("string", kafkaProducerFormats("string")))

  implicit val ex = system.dispatcher

  override def beforeAll() = {
    EmbeddedKafka.start()
    EmbeddedKafka.createCustomTopic("test_topic")
  }

  override def afterAll() = {
    system.stop(parent.ref)
    system.stop(kafkaActor)
    EmbeddedKafka.stop()
    TestKit.shutdownActorSystem(system)
  }

  describe("When Producing messages") {
    it("produces") {
      kafkaActor ! ProduceOnly(StringRecord("test_topic", Some("key"), "payload"))
      parent.expectMsgType[RecordProduced](15.seconds)
    }
    it("throws an exception") {
      val sr = StringRecord("unkown_topic", Some("key"), "payload")
      kafkaActor ! ProduceOnly(sr)
      parent.expectMsgPF(30.seconds) {
        case r: RecordNotProduced[_, _] =>
          r.record shouldBe sr
          r.error should not be null
      }
    }

    it("sends metadata back to the parent") {
      val kafkaActor = parent.childActorOf(KafkaProducerProxy.props("string", kafkaProducerFormats("string")))
      val kmd = KafkaRecordMetadata(recordMetadata, 0, DeliveryStrategy.AtMostOnce)
      kafkaActor ! kmd
      parent.expectMsgPF() {
        case RecordProduced(k) => k shouldBe kmd
      }
      kafkaActor ! PoisonPill
    }

    it("sends the error back to the parent") {
      val kafkaActor = parent.childActorOf(KafkaProducerProxy.props("string", kafkaProducerFormats("string")))
      val record = StringRecord("test_topic", Some("key"), "payload")
      val err = new IllegalArgumentException("ERROR")
      kafkaActor ! RecordNotProduced(record, err)
      parent.expectMsg(RecordNotProduced(record, err))
      kafkaActor ! PoisonPill
    }

    it("errors out with invalid producer config") {
      val cfg = ConfigFactory.parseString(
        """
          | acks = "1"
          | metadata.fetch.timeout.ms = 1000
          | key.serializer = "org.apache.kafka.common.serialization.StringSerializer"
          | key.deserializer = "org.apache.kafka.common.serialization.StringDeserializer"
          | value.serializer = "io.confluent.kafka.serializers.KafkaAvroSerializer"
          | value.deserializer = "io.confluent.kafka.serializers.KafkaAvroDeserializer"
          | client.id = "hydra.avro"
        """.stripMargin)
      val probe = TestProbe()
      val act = probe.childActorOf(KafkaProducerProxy.props("json", cfg))
      act ! ProduceOnly(JsonRecord("test_topic", Some("key"), """{"name":"alex"}"""))
      probe.expectMsgPF(10.seconds) {
        case ProducerInitializationError("json", ex) => ex shouldBe a[ConfigException]
      }
    }

  }

  private val recordMetadata = {
    val tp = new TopicPartition("topic", 0)
    new RecordMetadata(tp, 1L, 1L, 1L, 1L: java.lang.Long, 1, 1)
  }

}
