package hydra.kafka.transport

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import hydra.common.config.ConfigSupport
import hydra.core.transport.Transport.Deliver
import hydra.core.transport.{AckStrategy, RecordMetadata, TransportCallback}
import hydra.kafka.producer.{DeleteTombstoneRecord, JsonRecord, StringRecord}
import hydra.kafka.transport.KafkaProducerProxy.ProducerInitializationError
import hydra.kafka.transport.KafkaTransport.RecordProduceError
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.SerializationException
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.duration._

/**
  * Created by alexsilva on 12/5/16.
  */
class KafkaTransportSpec
    extends TestKit(ActorSystem("hydra"))
    with Matchers
    with AnyFunSpecLike
    with ImplicitSender
    with BeforeAndAfterAll
    with ConfigSupport {

  val producerName = StringRecord(
    "transport_test",
    Some("key"),
    "payload",
    AckStrategy.NoAck
  ).formatName

  lazy val transport = system.actorOf(KafkaTransport.props(rootConfig), "kafka")

  implicit val config = EmbeddedKafkaConfig(
    kafkaPort = 8012,
    zooKeeperPort = 3111,
    customBrokerProperties = Map("auto.create.topics.enable" -> "false")
  )

  val ingestor = TestProbe()
  val streamActor = TestProbe()

  system.eventStream.subscribe(streamActor.ref, classOf[RecordProduceError])
  system.eventStream
    .subscribe(streamActor.ref, classOf[ProducerInitializationError])

  override def beforeAll() = {
    super.beforeAll()
    EmbeddedKafka.start()
    EmbeddedKafka.createCustomTopic("transport_test")
  }

  override def afterAll() = {
    super.afterAll()
    system.stop(transport)
    EmbeddedKafka.stop()
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  describe("When using the KafkaTransport") {

    it("errors if no client can be found for the message") {
      val probe = TestProbe()
      val ack: TransportCallback =
        (d: Long, md: Option[RecordMetadata], err: Option[Throwable]) =>
          probe.ref ! err.get
      val rec = new StringRecord(
        "transport_test",
        "key",
        """{"name":"alex"}""",
        AckStrategy.NoAck
      ) {
        override val formatName: String = "unknown"
      }
      transport ! Deliver(rec, 1, ack)
      probe.expectMsgType[IllegalArgumentException](16.seconds)
    }

    it("forwards to the right proxy") {
      val ack: TransportCallback =
        (d: Long, m: Option[RecordMetadata], e: Option[Throwable]) =>
          ingestor.ref ! "DONE"
      val rec =
        StringRecord("transport_test", "key", "payload", AckStrategy.NoAck)
      transport ! Deliver(rec, 1, ack)
      ingestor.expectMsg(max = 10.seconds, "DONE")
    }

    it("handles delete records") {
      val ack: TransportCallback =
        (d: Long, m: Option[RecordMetadata], e: Option[Throwable]) =>
          ingestor.ref ! "DONE"
      val rec =
        DeleteTombstoneRecord("transport_test", "key", AckStrategy.NoAck)
      transport ! Deliver(rec, 1, ack)
      ingestor.expectMsg(max = 10.seconds, "DONE")
    }

    it("publishes errors to the stream") {
      val rec = JsonRecord(
        "transport_test",
        Some("key"),
        """{"name":"alex"}""",
        AckStrategy.NoAck
      )
      transport ! Deliver(rec)
      streamActor.expectMsgPF() {
        case RecordProduceError(deliveryId, r, err) =>
          deliveryId shouldBe -1
          r shouldBe rec
          err shouldBe a[SerializationException]
      }
    }

    it("publishes producer init errors to the stream") {

      val cfg = ConfigFactory.parseString(
        """
          |akka {
          |  kafka.producer {
          |    close-on-producer-stop = true
          |    parallelism = 100
          |    close-timeout = 60s
          |    use-dispatcher = test
          |    eos-commit-interval=100ms
          |    kafka-clients {
          |    }
          |  }
          |}
          |hydra_kafka {
          |
          |   schema.registry.url = "localhost:808"
          |   kafka.producer {
          |     bootstrap.servers="localhost:8092"
          |     key.serializer = org.apache.kafka.common.serialization.StringSerializer
          |   }
          |   kafka.clients {
          |      test.producer {
          |       value.serializer = io.confluent.kafka.serializers.KafkaAvroSerializer
          |      }
          |   }
          |}
          |
      """.stripMargin
      )

      system.actorOf(KafkaTransport.props(cfg))
      streamActor.expectMsgPF() {
        case ProducerInitializationError("test", err) =>
          err shouldBe a[KafkaException]
      }
    }
  }
}
