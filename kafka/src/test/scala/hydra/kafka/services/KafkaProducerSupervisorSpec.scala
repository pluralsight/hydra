package hydra.kafka.services

import akka.actor.{ActorSystem, PoisonPill}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import hydra.core.protocol.{Produce, RecordNotProduced, RecordProduced}
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.producer.{JsonRecord, KafkaRecordMetadata, StringRecord}
import info.batey.kafka.unit.KafkaUnit
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.duration._
import scala.util.Success

/**
  * Created by alexsilva on 12/5/16.
  */
class KafkaProducerSupervisorSpec extends TestKit(ActorSystem("hydra")) with Matchers with FunSpecLike with ImplicitSender
  with BeforeAndAfterAll with KafkaConfigSupport {

  val producerName = StringRecord("test_topic", Some("key"), "payload").formatName

  val kafkaSupervisor = TestActorRef[KafkaProducerSupervisor](KafkaProducerSupervisor.props(kafkaProducerFormats))

  val kafka = new KafkaUnit(3181, 8092)

  override def beforeAll() = {
    kafka.startup()
    kafka.createTopic("test_topic")
  }

  override def afterAll() = {
  //  kafkaSupervisor ! PoisonPill
//    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
    kafka.shutdown()

  }

  describe("When using the supervisor") {
    it("has the right producers") {
      kafkaSupervisor.underlyingActor.producers.size shouldBe 2
      kafkaSupervisor.underlyingActor.producers.keys should contain allOf("avro", "string")
    }

    it("errors if no proxy can be found for the format") {
      kafkaSupervisor ! Produce(JsonRecord("test_topic", Some("key"), """{"name":"alex"}"""))
      expectMsgPF(max = 5.seconds) {
        case RecordNotProduced(record: JsonRecord, error) =>
          error shouldBe an[IllegalArgumentException]
          record.destination shouldBe "test_topic"
      }
    }

    ignore("forwards to the right proxy") {
      import akka.pattern.ask
      implicit val ti = Timeout(15.seconds)
      val future = kafkaSupervisor ? Produce(StringRecord("test_topic", Some("key"), "payload"))
      val Success(result: RecordProduced) = future.value.get
      result.md.asInstanceOf[KafkaRecordMetadata].topic shouldBe "test-topic"
      result.md.asInstanceOf[KafkaRecordMetadata].partition shouldBe 0
    }
  }

}
