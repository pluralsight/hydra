package hydra.kafka.transport

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import hydra.core.protocol.{RecordNotProduced, RecordProduced}
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.producer.StringRecord
import hydra.kafka.transport.KafkaProducerProxy.ProduceToKafka
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpecLike, Matchers}

import scala.concurrent.duration._

/**
  * Created by alexsilva on 12/5/16.
  */
class KafkaTransportProxySpec extends TestKit(ActorSystem("hydra")) with Matchers with FunSpecLike with ImplicitSender
  with BeforeAndAfterAll with BeforeAndAfterEach with KafkaConfigSupport with EmbeddedKafka{

  implicit val config = EmbeddedKafkaConfig(kafkaPort = 8092, zooKeeperPort = 33181,
    customBrokerProperties = Map("auto.create.topics.enable" -> "false"))

  val parent = TestProbe()
  val kafkaActor = TestActorRef(KafkaProducerProxy.props(parent.ref, "string"))

  implicit val ex = system.dispatcher

  override def beforeAll() = {
    EmbeddedKafka.start()
    EmbeddedKafka.createCustomTopic("test_topic")
  }

  override def afterAll() = {
    EmbeddedKafka.stop()
    TestKit.shutdownActorSystem(system)
  }

  describe("When creating a props object") {
    it("throws an error if format is unknown") {
      intercept[InvalidProducerSettingsException] {
        KafkaProducerProxy.props(parent.ref, "unknown-format")
      }
    }

    it("returns a configured producer") {
      KafkaProducerProxy.props(parent.ref, "string").args should contain
      Seq(parent, kafkaProducerFormats("string"))

    }
  }
  describe("When Producing messages") {
    it("produces") {
      kafkaActor ! ProduceToKafka(StringRecord("test_topic", Some("key"), "payload"),0)
      parent.expectMsgType[RecordProduced](15.seconds)
    }
    it("throws an exception") {
      kafkaActor ! ProduceToKafka(StringRecord("unkown_topic", Some("key"), "payload"),0)
      parent.expectMsgType[RecordNotProduced[String, String]](15.seconds)
    }
  }

}
