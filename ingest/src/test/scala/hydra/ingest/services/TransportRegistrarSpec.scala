package hydra.ingest.services

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.typesafe.config.Config
import hydra.common.config.ConfigSupport
import hydra.core.transport.Transport
import hydra.core.transport.TransportSupervisor.Deliver
import hydra.ingest.services.TransportRegistrar._
import hydra.ingest.test.TestRecord
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

class TransportRegistrarSpec extends TestKit(ActorSystem("test")) with Matchers
  with FunSpecLike with BeforeAndAfterAll with ImplicitSender with ConfigSupport {

  val tr = TestActorRef[TransportRegistrar](Props[TransportRegistrar], "transport_registrar")

  override def afterAll = TestKit.shutdownActorSystem(system)


  describe("The Transport Registrar") {
    it("bootstraps from a package") {

    }
    it("registers a transport using its companion object") {
      val transports = bootstrap(Map("transport_test1" -> classOf[TransportTest]), system, applicationConfig)
      transports(0).get ! Deliver(TestRecord("transport_test", Some("key"), """{"name":"alex"}"""))
      expectMsg("HELLO!") //defined in reference.conf
    }

    it("registers a transport without a companion object") {
      val transports = bootstrap(Map("transport_test2" -> classOf[CompanionLessTransportTest]), system,
        applicationConfig)
      transports(0).get ! Deliver(TestRecord("transport_test", Some("key"), """{"name":"alex"}"""))
      expectMsg("HI!")
    }

    it("reports the error if it can't instantiate") {
      val transports = bootstrap(Map("transport_test3" -> classOf[ErrorTransport]), system, applicationConfig)
      intercept[IllegalArgumentException] {
        transports(0).get
      }
    }
  }
}

class ErrorTransport(map: Map[String, String]) extends Transport {
  override def receive: Receive = {
    case Deliver(record, deliveryId, callback) => //ignore
  }
}

class TransportTest(config: Config) extends Transport {
  override def receive: Receive = {
    case Deliver(record, deliveryId, callback) =>
      sender ! config.getString("transports.test.message")
  }
}

object TransportTest {
  def props(config: Config) = Props(new TransportTest(config))
}

class CompanionLessTransportTest extends Transport {
  override def receive: Receive = {
    case Deliver(record, deliveryId, callback) =>
      sender ! "HI!"
    case x =>
      println(x)
  }
}