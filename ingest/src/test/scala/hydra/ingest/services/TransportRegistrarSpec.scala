package hydra.ingest.services

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.typesafe.config.Config
import hydra.common.config.ConfigSupport
import hydra.core.transport.{AckStrategy, Transport}
import hydra.core.transport.Transport.Deliver
import hydra.ingest.services.TransportRegistrar._
import hydra.ingest.test.TestRecord
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.duration._

class TransportRegistrarSpec
    extends TestKit(ActorSystem("test"))
    with Matchers
    with AnyFunSpecLike
    with BeforeAndAfterAll
    with ImplicitSender
    with ConfigSupport {

  val tr = TestActorRef[TransportRegistrar](
    Props[TransportRegistrar],
    "transport_registrar"
  )

  override def afterAll =
    TestKit.shutdownActorSystem(
      system,
      verifySystemShutdown = true,
      duration = 10 seconds
    )

  describe("The Transport Registrar") {
    it("bootstraps from a package") {}
    it("registers a transport using its companion object") {
      val transports = bootstrap(
        Map("transport_test1" -> classOf[TransportTest]),
        system,
        applicationConfig
      )
      transports(0).get ! Deliver(
        TestRecord(
          "transport_test",
          "key",
          """{"name":"alex"}""",
          AckStrategy.NoAck
        )
      )
      expectMsg("HELLO!") //defined in reference.conf
    }

    it("registers a transport without a companion object") {
      val transports = bootstrap(
        Map("transport_test2" -> classOf[CompanionLessTransportTest]),
        system,
        applicationConfig
      )
      transports(0).get ! Deliver(
        TestRecord(
          "transport_test",
          "key",
          """{"name":"alex"}""",
          AckStrategy.NoAck
        )
      )
      expectMsg("HI!")
    }

    it("reports the error if it can't instantiate") {
      val transports = bootstrap(
        Map("transport_test3" -> classOf[ErrorTransport]),
        system,
        applicationConfig
      )
      intercept[IllegalArgumentException] {
        transports(0).get
      }
    }

    it("returns all transports") {
      tr ! GetTransports
      expectMsgPF() {
        case r: GetTransportsResponse =>
          r.transports should contain allOf ("transport_test",
          "companion_less_transport_test", "error_transport", "test_transport")
      }
    }
  }
}

class ErrorTransport(map: Map[String, String]) extends Transport {

  override def transport: Receive = {
    case Deliver(record, deliveryId, callback) => //ignore
  }
}

class TransportTest(config: Config) extends Transport {

  override def transport: Receive = {
    case Deliver(record, deliveryId, callback) =>
      sender ! config.getString("transports.test.message")
  }
}

object TransportTest {
  def props(config: Config) = Props(new TransportTest(config))
}

class CompanionLessTransportTest extends Transport {

  override def transport: Receive = {
    case Deliver(record, deliveryId, callback) => sender ! "HI!"
  }
}
