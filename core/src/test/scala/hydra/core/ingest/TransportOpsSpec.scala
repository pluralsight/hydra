package hydra.core.ingest

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import hydra.core.protocol.{IngestorCompleted, IngestorError}
import hydra.core.test.TestRecordFactory
import hydra.core.transport.AckStrategy.NoAck
import hydra.core.transport._
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * Created by alexsilva on 3/22/17.
  */
class TransportOpsSpec extends TestKit(ActorSystem("test")) with Matchers with FunSpecLike
  with BeforeAndAfterAll with ImplicitSender {

  val transportAct = system.actorOf(Props[TransportTester], "test-transport_transport")

  override def afterAll() = TestKit.shutdownActorSystem(system)

  val supervisor = TestProbe()

  describe("TransportOps") {
    it("looks up a transport") {
      val t = system.actorOf(Props(classOf[TestTransportIngestor], supervisor.ref))
      t ! "hello"
      expectMsg("hi!")
    }

    it("won't initialize if transport can't be found") {
      val t = system.actorOf(Props[TestTransportIngestorError])
      t ! "hello"
      expectMsgPF() {
        case i: IngestorError =>
          i.error shouldBe a[IllegalArgumentException]
      }
    }

    it("transports a record") {
      val req = HydraRequest(123, "test-produce")
      val t = system.actorOf(Props(classOf[TestTransportIngestor], supervisor.ref))
      t ! req
      supervisor.expectMsg(IngestorCompleted)
    }

    it("sends a produce error") {
      val req = HydraRequest(123, "test-error-produce")
      val t = system.actorOf(Props(classOf[TestTransportIngestor], supervisor.ref))
      t ! req
      supervisor.expectMsgPF() {
        case IngestorError(c, err) =>
          c shouldBe -2
          err.getMessage shouldBe "test-error-produce"
      }
    }
  }
}


class TestTransportIngestor(supervisor: ActorRef) extends Ingestor with TransportOps {

  override val recordFactory = TestRecordFactory

  ingest {
    case "hello" => sender ! "hi!"
    case req: HydraRequest => transport(TestRecordFactory.build(req).get, supervisor, NoAck)
  }

  override def transportName = "test-transport"
}

class TestTransportIngestorError extends Ingestor with TransportOps {

  override val recordFactory = TestRecordFactory

  override def transportName = "test-transport-unknown"
}
