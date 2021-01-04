package hydra.kafka.transport

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import hydra.common.config.ConfigSupport
import hydra.core.protocol.{RecordNotProduced, RecordProduced}
import hydra.core.transport
import hydra.core.transport.{AckStrategy, HydraRecord, NoCallback, TransportCallback}
import hydra.kafka.producer.{JsonRecord, KafkaRecordMetadata, StringRecord}
import hydra.kafka.transport.KafkaProducerProxy.ProduceToKafka
import hydra.kafka.transport.KafkaTransport.RecordProduceError
import hydra.kafka.util.KafkaUtils
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

/**
  * Created by alexsilva on 12/5/16.
  */
class KafkaProducerProxySpec
    extends TestKit(ActorSystem("KafkaProducerProxySpec"))
    with Matchers
    with AnyFunSpecLike
    with ImplicitSender
    with BeforeAndAfterAll
    with ConfigSupport {

  implicit val config = EmbeddedKafkaConfig(
    kafkaPort = 8012,
    zooKeeperPort = 3111,
    customBrokerProperties = Map("auto.create.topics.enable" -> "false")
  )

  private val parent = TestProbe()

  private val settings = KafkaUtils.producerSettings("string", rootConfig)

  private def kafkaProducer =
    parent.childActorOf(KafkaProducerProxy.props("string", settings))

  implicit private val ex = system.dispatcher

  private val ingestor = TestProbe()
  private val supervisor = TestProbe()

  override def beforeAll() = {
    super.beforeAll()
    EmbeddedKafka.start()
    EmbeddedKafka.createCustomTopic("kafka_producer_spec")
  }

  override def afterAll() = {
    super.afterAll()
    EmbeddedKafka.stop()
    system.stop(kafkaProducer)
    TestKit.shutdownActorSystem(system)
  }

  private def callback(record: HydraRecord[_, _]): TransportCallback =
    (
        deliveryId: Long,
        md: Option[transport.RecordMetadata],
        exception: Option[Throwable]
    ) => {
      val msg = md
        .map(RecordProduced(_, supervisor.ref))
        .getOrElse(RecordNotProduced(record, exception.get, supervisor.ref))

      ingestor.ref ! msg
    }

  describe("When Producing messages") {
    it("produces without acking") {
      val record = StringRecord(
        "kafka_producer_spec",
        Some("key"),
        "payload",
        AckStrategy.NoAck
      )
      kafkaProducer ! ProduceToKafka(10, record, NoCallback)
      parent.expectMsgPF(10.seconds) {
        case KafkaRecordMetadata(
            offset,
            ts,
            "kafka_producer_spec",
            part,
            deliveryId,
            AckStrategy.NoAck
            ) =>
          deliveryId shouldBe 10
          offset should be >= 0L
          ts should be > 0L
          part shouldBe 0
      }
    }

    it("acks") {
      val record = StringRecord(
        "kafka_producer_spec",
        Some("key"),
        "payload",
        AckStrategy.NoAck
      )
      kafkaProducer ! ProduceToKafka(123, record, callback(record))
      parent.expectMsgPF(15.seconds) {
        case md: KafkaRecordMetadata =>
          md.destination shouldBe "kafka_producer_spec"
          md.deliveryId shouldBe 123
      }

      ingestor.expectMsgPF() {
        case RecordProduced(
            KafkaRecordMetadata(
              offset,
              _,
              "kafka_producer_spec",
              0,
              deliveryId,
              AckStrategy.NoAck
            ),
            sup
            ) =>
          deliveryId shouldBe 123
          offset should be >= 0L
          sup shouldBe supervisor.ref
      }
    }

    it("acks the produce error") {
      val record =
        StringRecord(null, Some("key"), "test-error-payload", AckStrategy.NoAck)
      kafkaProducer ! ProduceToKafka(123, record, callback(record))
      parent.expectMsgPF() {
        case err: RecordProduceError =>
          err.deliveryId shouldBe 123
          err.record shouldBe record
          err.error shouldBe a[IllegalArgumentException]
      }

      ingestor.expectMsgPF() {
        case RecordNotProduced(r, err, sup) =>
          err shouldBe a[IllegalArgumentException]
          r shouldBe record
          sup shouldBe supervisor.ref
      }
    }

    it("sends metadata back to the parent") {
      val kmd = KafkaRecordMetadata(recordMetadata, 0, AckStrategy.NoAck)
      kafkaProducer ! kmd
      parent.expectMsg(kmd)
    }

    it("sends the error back to the parent") {
      val record = StringRecord(
        "kafka_producer_spec",
        Some("key"),
        "payload",
        AckStrategy.NoAck
      )
      val err = new IllegalArgumentException("ERROR")
      kafkaProducer ! RecordProduceError(123, record, err)
      parent.expectMsg(RecordProduceError(123, record, err))
    }

    it("errors out with invalid message client id") {
      val probe = TestProbe()
      val act = probe.childActorOf(
        KafkaProducerProxy
          .props("tester", KafkaUtils.producerSettings("tester", rootConfig))
      )
      val record = JsonRecord(
        "kafka_producer_spec",
        Some("key"),
        """{"name":"alex"}""",
        AckStrategy.NoAck
      )
      act ! ProduceToKafka(0, record, callback(record))
      probe.expectMsgPF(10.seconds) {
        case RecordProduceError(0, rec, ex) =>
          rec shouldBe record
          ex shouldBe a[KafkaException]
      }
    }
  }

  private val recordMetadata = {
    val tp = new TopicPartition("topic", 0)
    new RecordMetadata(tp, 1L, 1L, 1L, 1L: java.lang.Long, 1, 1)
  }

}
