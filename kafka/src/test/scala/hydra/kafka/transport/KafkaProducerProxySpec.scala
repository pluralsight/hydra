package hydra.kafka.transport

import akka.actor.{ActorSystem, PoisonPill}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import hydra.core.protocol.{RecordNotProduced, RecordProduced}
import hydra.core.transport.AckStrategy
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.producer.{JsonRecord, KafkaRecordMetadata, StringRecord}
import hydra.kafka.transport.KafkaProducerProxy.{ProduceToKafka, ProducerInitializationError}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.scalatest._

import scala.concurrent.duration._

/**
  * Created by alexsilva on 12/5/16.
  */
@DoNotDiscover
class KafkaProducerProxySpec extends TestKit(ActorSystem("hydra")) with Matchers with FunSpecLike with ImplicitSender
  with BeforeAndAfterAll with BeforeAndAfterEach with KafkaConfigSupport {

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
    TestKit.shutdownActorSystem(system)
  }

  describe("When Producing messages") {
    it("produces without acking") {
      kafkaActor ! ProduceToKafka(0, StringRecord("test_topic", Some("key"), "payload"),
        null, TestProbe().ref, AckStrategy.NoAck)
      parent.expectMsgType[RecordProduced](15.seconds)
    }

    it("acks") {
      val record = StringRecord("test_topic", Some("key"), "payload")
      val i = TestProbe()
      val s = TestProbe()
      kafkaActor ! ProduceToKafka(123, record, i.ref.path, s.ref, AckStrategy.TransportAck)
      parent.expectMsgType[RecordProduced](15.seconds)
      i.expectMsgPF() {
        case RecordProduced(md: KafkaRecordMetadata, s) =>
          md.topic shouldBe "test_topic"
          md.deliveryId shouldBe 123
          s shouldBe Some(s)
      }
    }

    it("sends metadata back to the parent") {
      val kafkaActor = parent.childActorOf(KafkaProducerProxy.props("string", kafkaProducerFormats("string")))
      val kmd = KafkaRecordMetadata(recordMetadata, 0)
      kafkaActor ! kmd
      parent.expectMsg(kmd)
      kafkaActor ! PoisonPill
    }

    it("sends the error back to the parent") {
      val kafkaActor = parent.childActorOf(KafkaProducerProxy.props("string", kafkaProducerFormats("string")))
      val record = StringRecord("test_topic", Some("key"), "payload")
      val err = new IllegalArgumentException("ERROR")
      kafkaActor ! RecordNotProduced(111, record, err, null)
      parent.expectMsg(RecordNotProduced(111, record, err, null))
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
      act ! ProduceToKafka(0, JsonRecord("test_topic", Some("key"), """{"name":"alex"}"""),
        probe.ref.path, probe.ref, AckStrategy.NoAck)
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
