package hydra.kafka.transport

import akka.actor.{ActorSystem, PoisonPill}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import hydra.core.protocol.{RecordNotProduced, RecordProduced}
import hydra.core.transport.Transport.AckCallback
import hydra.core.transport.{HydraRecord, Transport}
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.producer.{JsonRecord, KafkaRecordMetadata, StringRecord}
import hydra.kafka.transport.KafkaProducerProxy.{ProduceToKafka, ProducerInitializationError}
import hydra.kafka.transport.KafkaTransport.RecordProduceError
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.TimeoutException
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

  private val parent = TestProbe()

  private val kafkaActor = parent.childActorOf(KafkaProducerProxy.props("string", kafkaProducerFormats("string")))

  implicit private val ex = system.dispatcher

  private val ingestor = TestProbe()
  private val supervisor = TestProbe()

  override def beforeAll() = {
    EmbeddedKafka.createCustomTopic("test_topic")
  }

  override def afterAll() = {
    system.stop(parent.ref)
    system.stop(kafkaActor)
    TestKit.shutdownActorSystem(system)
  }

  private def callback(record: HydraRecord[_, _]): AckCallback =
    (md, err) => ingestor.ref ! (md.map(RecordProduced(_, supervisor.ref))
      .getOrElse(RecordNotProduced(record, err.get, supervisor.ref)))


  describe("When Producing messages") {
    it("produces without acking") {
      val record = StringRecord("test_topic", Some("key"), "payload")
      kafkaActor ! ProduceToKafka(10, record, Transport.NoAck)
      parent.expectMsgPF(10.seconds) {
        case KafkaRecordMetadata(offset, ts, "test_topic", part, deliveryId) =>
          deliveryId shouldBe 10
          offset should be >= 0L
          ts should be > 0L
          part shouldBe 0

      }
    }

    it("acks") {
      val record = StringRecord("test_topic", Some("key"), "payload")
      kafkaActor ! ProduceToKafka(123, record, callback(record))
      parent.expectMsgPF(15.seconds) {
        case md: KafkaRecordMetadata =>
          md.topic shouldBe "test_topic"
          md.deliveryId shouldBe 123
      }

      ingestor.expectMsgPF() {
        case RecordProduced(KafkaRecordMetadata(offset, _, "test_topic", 0, deliveryId), sup) =>
          deliveryId shouldBe 123
          offset should be >= 0L
          sup shouldBe supervisor.ref
      }
    }

    it("acks the produce error") {
      val record = StringRecord("unknown_topic", Some("key"), "payload")
      kafkaActor ! ProduceToKafka(123, record, callback(record))
      parent.expectMsgPF(15.seconds) {
        case err: RecordProduceError =>
          err.deliveryId shouldBe 123
          err.record shouldBe record
          err.error shouldBe a[TimeoutException]
      }

      ingestor.expectMsgPF() {
        case RecordNotProduced(r, err, sup) =>
          err shouldBe a[TimeoutException]
          r shouldBe record
          sup shouldBe supervisor.ref
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
      kafkaActor ! RecordProduceError(123, record, err)
      parent.expectMsg(RecordProduceError(123, record, err))
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
      val record = JsonRecord("test_topic", Some("key"), """{"name":"alex"}""")
      act ! ProduceToKafka(0, record, callback(record))
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