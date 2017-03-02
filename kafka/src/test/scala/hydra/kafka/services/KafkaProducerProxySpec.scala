package hydra.kafka.services

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import hydra.core.protocol.{Produce, RecordNotProduced, RecordProduced}
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.producer.StringRecord
import info.batey.kafka.unit.KafkaUnit
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpecLike, Matchers}

import scala.concurrent.duration._

/**
  * Created by alexsilva on 12/5/16.
  */
class KafkaProducerProxySpec extends TestKit(ActorSystem("hydra")) with Matchers with FunSpecLike with ImplicitSender
  with BeforeAndAfterAll with BeforeAndAfterEach with KafkaConfigSupport {

  val kafka = new KafkaUnit(3181, 8092)
  kafka.setKafkaBrokerConfig("auto.create.topics.enable", "false")
  kafka.startup()

  val parent = TestProbe()
  val kafkaActor = TestActorRef(KafkaProducerProxy.props(parent.ref, "string"))

  implicit val ex = system.dispatcher

  override def beforeAll() = {
    kafka.createTopic("test_topic")
  }

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
    kafka.shutdown()
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
      kafkaActor ! Produce(StringRecord("test_topic", Some("key"), "payload"))
      parent.expectMsgType[RecordProduced](15.seconds)
    }
    it("throws an exception") {
      kafkaActor ! Produce(StringRecord("unkown_topic", Some("key"), "payload"))
      parent.expectMsgType[RecordNotProduced[String, String]](15.seconds)
    }
  }

}
