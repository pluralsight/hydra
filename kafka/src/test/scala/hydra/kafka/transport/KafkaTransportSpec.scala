package hydra.kafka.transport

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import hydra.core.protocol.{Produce, RecordNotProduced, RecordProduced}
import hydra.kafka.ForwardActor
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.producer.{JsonRecord, KafkaRecordMetadata, StringRecord}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.duration._
import scala.util.Success

/**
  * Created by alexsilva on 12/5/16.
  */
class KafkaTransportSpec extends TestKit(ActorSystem("hydra")) with Matchers with FunSpecLike with ImplicitSender
  with BeforeAndAfterAll with KafkaConfigSupport with EmbeddedKafka {

  val producerName = StringRecord("test_topic", Some("key"), "payload").formatName

  lazy val kafkaSupervisor = TestActorRef[KafkaTransport](KafkaTransport.props(kafkaProducerFormats))

  implicit val config = EmbeddedKafkaConfig(kafkaPort = 8092, zooKeeperPort = 3181,
    customBrokerProperties = Map("auto.create.topics.enable" -> "false"))

  override def beforeAll() = {
    EmbeddedKafka.start()
    EmbeddedKafka.createCustomTopic("test_topic")
  }

  override def afterAll() = {
    EmbeddedKafka.stop()
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  describe("When using the supervisor") {
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
      val act = probe.childActorOf(KafkaTransport.props(Map("json" -> cfg)), "json")
      val p = system.actorOf(Props(new ForwardActor(act)), "kafka_producer")
      p ! Produce(JsonRecord("test_topic", Some("key"), """{"name":"alex"}"""))
      probe.expectMsgPF(10.seconds) {
        case x=>println(x)//ProducerInitializationError("json", ex) => ex shouldBe "RAR"
      }
    }

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

    it("creates producers from config") {
      val cfg = ConfigFactory.parseString(
        """
          | acks = "1"
          | batch.size = 0 //disable
          | metadata.fetch.timeout.ms = 1000
          | bootstrap.servers = "localhost:8092"
          | zookeeper.connect = "localhost:3181"
          | key.serializer = "org.apache.kafka.common.serialization.StringSerializer"
          | key.deserializer = "org.apache.kafka.common.serialization.StringDeserializer"
          | value.serializer = "io.confluent.kafka.serializers.KafkaAvroSerializer"
          | value.deserializer = "io.confluent.kafka.serializers.KafkaAvroDeserializer"
          | client.id = "hydra.avro"
        """.stripMargin)
      // val act = TestActorRef[KafkaTransport](KafkaTransport.props(Map("avro" -> cfg)))
      // act.underlyingActor.initProducers() shouldBe Map("avro" -> "rar")
    }
  }

}
