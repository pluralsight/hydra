package hydra.core.ingest

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import hydra.core.protocol.{IngestorCompleted, IngestorError, ValidRequest, WaitingForAck}
import hydra.core.transport._
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.util.Success

/**
  * Created by alexsilva on 3/22/17.
  */
class TransportOpsSpec extends TestKit(ActorSystem("test")) with Matchers with FunSpecLike
  with BeforeAndAfterAll with ImplicitSender {

  val transportAct = system.actorOf(Props[TransportTester], "test-transport_transport")

  override def afterAll() = TestKit.shutdownActorSystem(system)

  describe("TransportOps") {
    it("looks up a transport") {
      val t = system.actorOf(Props[TestTransportIngestor])
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
      val req = HydraRequest(123, "test")
      val t = system.actorOf(Props[TestTransportIngestor])
      t ! req
      expectMsg(IngestorCompleted)
    }

    it("acknowledges requests with explicit acks") {
      val req = HydraRequest(123, "test").withAckStrategy(AckStrategy.Explicit)
      val t = system.actorOf(Props[TestTransportIngestor])
      t ! req
      expectMsg(WaitingForAck)
    }
  }
}

object TestRecordFactory extends RecordFactory[String, String] {
  override def build(r: HydraRequest) = Success(new HydraRecord[String, String] {
    override def destination = "test-topic"

    override def key = Some(r.correlationId.toString)

    override def payload = r.payload

    override def retryStrategy = RetryStrategy.Ignore
  })

  override def validate(request: HydraRequest) = ValidRequest
}

class TestTransportIngestor extends Ingestor with TransportOps {

  ingest {
    case "hello" => sender ! "hi!"
    case req: HydraRequest => sender ! transport(req)(TestRecordFactory)
  }

  override def transportName = "test-transport"
}

class TestTransportIngestorError extends Ingestor with TransportOps {

  override def transportName = "test-transport-unknown"
}
