package hydra.core.ingest

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.TestActors.ForwardActor
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.pluralsight.hydra.reflect.DoNotScan
import hydra.core.akka.ActorInitializationException
import hydra.core.protocol.{IngestorError, Produce}
import hydra.core.test.TestRecordFactory
import hydra.core.transport.AckStrategy.NoAck
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * Created by alexsilva on 3/22/17.
  */
class TransportOpsSpec
    extends TestKit(ActorSystem("test"))
    with Matchers
    with AnyFunSpecLike
    with BeforeAndAfterAll
    with ImplicitSender
    with ScalaFutures {

  override def afterAll() = TestKit.shutdownActorSystem(system)

  val supervisor = TestProbe()

  val tm = TestProbe()

  val transport =
    system.actorOf(Props(new ForwardActor(tm.ref)), "test-transport")

  describe("TransportOps") {
    it("looks up a transport") {
      val t =
        system.actorOf(Props(classOf[TestTransportIngestor], supervisor.ref))
      t ! "hello"
      expectMsg("hi!")
    }

    it("won't initialize if transport can't be found") {
      val t = system.actorOf(Props[TestTransportIngestorError])
      t ! "hello"
      expectNoMessage()
    }

    it("transports a record") {
      val req = HydraRequest("123", "test-produce")
      val t =
        system.actorOf(Props(classOf[TestTransportIngestor], supervisor.ref))
      t ! req
      whenReady(TestRecordFactory.build(req))(r =>
        tm.expectMsg(Produce(r, self, NoAck))
      )
    }
  }
}

@DoNotScan
class TestTransportIngestor(supervisor: ActorRef)
    extends Ingestor
    with TransportOps {

  override val recordFactory = TestRecordFactory

  override def initTimeout = 500 millis

  ingest {
    case "hello" => sender ! "hi!"
    case req: HydraRequest =>
      val record = Await.result(TestRecordFactory.build(req), 3.seconds)
      transport(record, NoAck)
  }

  override def transportName = "test-transport"
}

class TestTransportIngestorError extends Ingestor with TransportOps {
  override val recordFactory = TestRecordFactory

  override def transportName = "test-transport-unknown"
}
