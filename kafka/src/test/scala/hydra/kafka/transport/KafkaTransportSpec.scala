package hydra.kafka.transport

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import hydra.core.protocol.{Produce, ProducerAck, RecordNotProduced}
import hydra.core.transport.AckStrategy
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.producer.{JsonRecord, StringRecord}
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
      kafkaSupervisor ! Produce(JsonRecord("transport_test", Some("key"), """{"name":"alex"}"""), ingestor.ref, supervisor.ref)
      expectMsgPF(max = 5.seconds) {
        case RecordNotProduced(record: JsonRecord, error) =>
          error shouldBe an[IllegalArgumentException]
          record.destination shouldBe "transport_test"
      }
    }

    it("forwards to the right proxy") {
      val rec = StringRecord("transport_test", Some("key"), "payload", ackStrategy = AckStrategy.Explicit)
      kafkaSupervisor ! Produce(rec, ingestor.ref, supervisor.ref)
      ingestor.expectMsgPF(max = 10.seconds) {
        case ProducerAck(s, e) =>
          s shouldBe supervisor.ref
          e shouldBe None
      }
    }
  }

}
