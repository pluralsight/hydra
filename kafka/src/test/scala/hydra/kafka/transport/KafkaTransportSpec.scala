package hydra.kafka.transport

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import hydra.core.protocol.{Produce, RecordAccepted, RecordNotProduced, RecordProduced}
import hydra.core.transport.{AckStrategy, HydraRecordMetadata}
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.producer.{JsonRecord, KafkaRecordMetadata, StringRecord}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, FunSpecLike, Matchers}

import scala.concurrent.duration._

/**
  * Created by alexsilva on 12/5/16.
  */
@DoNotDiscover
class KafkaTransportSpec extends TestKit(ActorSystem("hydra")) with Matchers with FunSpecLike with ImplicitSender
  with BeforeAndAfterAll with KafkaConfigSupport {

  val producerName = StringRecord("transport_test", Some("key"), "payload").formatName

  val kafkaSupervisor = TestActorRef[KafkaTransport](KafkaTransport.props(kafkaProducerFormats))

  implicit val config = EmbeddedKafkaConfig(kafkaPort = 8092, zooKeeperPort = 3181,
    customBrokerProperties = Map("auto.create.topics.enable" -> "false"))

  val ingestor = TestProbe()
  val supervisor = TestProbe()

  override def beforeAll() = {
    EmbeddedKafka.start()
    EmbeddedKafka.createCustomTopic("transport_test")
  }

  override def afterAll() = {
    system.stop(kafkaSupervisor)
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  describe("When using the supervisor") {

    it("has the right producers") {
      kafkaSupervisor.underlyingActor.producers.size shouldBe 2
      kafkaSupervisor.underlyingActor.producers.keys should contain allOf("avro", "string")
    }

    it("errors if no proxy can be found for the format") {
      kafkaSupervisor ! Produce(JsonRecord("transport_test", Some("key"), """{"name":"alex"}"""),
        supervisor.ref, AckStrategy.NoAck)
      expectMsgPF(max = 5.seconds) {
        case RecordNotProduced(_, record: JsonRecord, error, _) =>
          error shouldBe an[IllegalArgumentException]
          record.destination shouldBe "transport_test"
      }
    }

    it("forwards to the right proxy with no ack") {
      val rec = StringRecord("transport_test", Some("key"), "payload")
      kafkaSupervisor.tell(Produce(rec, supervisor.ref, AckStrategy.NoAck), ingestor.ref)
      ingestor.expectMsgPF(max = 10.seconds) {
        case RecordAccepted(sup) =>
          sup shouldBe supervisor.ref
      }
    }

    it("forwards to the right proxy and replies with local ack") {
      val rec = StringRecord("transport_test", Some("key"), "payload")
      kafkaSupervisor.tell(Produce(rec, supervisor.ref, AckStrategy.LocalAck, 1234), ingestor.ref)
      ingestor.expectMsgPF(max = 10.seconds) {
        case RecordProduced(md, s) =>
          s shouldBe supervisor.ref
          md shouldBe a[HydraRecordMetadata]
          val kmd = md.asInstanceOf[HydraRecordMetadata]
          kmd.deliveryId shouldBe 1234
          kmd.timestamp should be > 0L
      }
    }

    it("forwards to the right proxy and replies with transport ack") {
      val rec = StringRecord("transport_test", Some("key"), "payload")
      kafkaSupervisor.tell(Produce(rec, supervisor.ref, AckStrategy.TransportAck), ingestor.ref)
      ingestor.expectMsgPF(max = 10.seconds) {
        case RecordProduced(md, s) =>
          s shouldBe supervisor.ref
          md shouldBe a[KafkaRecordMetadata]
          val kmd = md.asInstanceOf[KafkaRecordMetadata]
          kmd.offset should be >= 0L
          kmd.topic shouldBe "transport_test"
      }
    }
  }

}
