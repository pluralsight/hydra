package hydra.core.ingest

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.TestActors.ForwardActor
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import hydra.core.akka.ActorInitializationException
import hydra.core.protocol.{IngestorError, Produce}
import hydra.core.test.TestRecordFactory
import hydra.core.transport.AckStrategy.NoAck
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.duration._

/**
  * Created by alexsilva on 3/22/17.
  */
class TransportOpsSpec extends TestKit(ActorSystem("test")) with Matchers with FunSpecLike
  with BeforeAndAfterAll with ImplicitSender {

  override def afterAll() = TestKit.shutdownActorSystem(system)

  val supervisor = TestProbe()

  val tm = TestProbe()

  val transport = system.actorOf(Props(new ForwardActor(tm.ref)), "test-transport")


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
          i.error shouldBe a[ActorInitializationException]
      }
    }

    it("transports a record") {
      val req = HydraRequest(123, "test-produce")
      val t = system.actorOf(Props(classOf[TestTransportIngestor], supervisor.ref))
      t ! req
      tm.expectMsg(Produce(TestRecordFactory.build(req).get, supervisor.ref, NoAck))
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
  override def initTimeout = 1.second

  override val recordFactory = TestRecordFactory

  override def transportName = "test-transport-unknown"
}
