package hydra.sandbox.ingest

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import hydra.common.config.ConfigSupport
import hydra.core.ingest.HydraRequest
import hydra.core.protocol._
import hydra.core.transport.AckStrategy
import hydra.sandbox.transport.FileRecord
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class FileIngestorSpec extends TestKit(ActorSystem("hydra-sandbox-test")) with Matchers with FunSpecLike
  with ImplicitSender
  with ConfigSupport
  with BeforeAndAfterAll
  with Eventually
  with ScalaFutures {

  val transportProbe = TestProbe()

  val fileProducer = system.actorOf(Props(new ForwardActor(transportProbe.ref)), "file_producer")

  val ingestor = transportProbe.childActorOf(Props[FileIngestor])

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(60, Seconds)), interval = scaled(Span(60, Millis)))

  override def afterAll = {
    system.stop(ingestor)
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  describe("The FileIngestor") {
    it("ignores") {
      val hr = HydraRequest("0", "test")
      ingestor ! Publish(hr)
      eventually {
        expectMsg(60.seconds, Ignore)
      }
    }

    it("validates") {
      val hr = HydraRequest("0", "test").withMetadata("hydra-file-stream" -> "test")
      ingestor ! Validate(hr)
      whenReady(FileRecordFactory.build(hr)) { r => expectMsg(ValidRequest(r)) }
      val hr1 = HydraRequest("0", "test").withMetadata("hydra-file-stream" -> "unknown")
      ingestor ! Validate(hr1)
      expectMsgPF() {
        case InvalidRequest(ex) =>
          ex shouldBe a[IllegalArgumentException]
      }
    }

    it("joins") {
      val hr = HydraRequest("0", "test").withMetadata("hydra-file-stream" -> "test")
      ingestor ! Publish(hr)
      eventually {
        expectMsg(60.seconds, Join)
      }
    }

    it("transports") {
      val hr = HydraRequest("0", "test").withMetadata("hydra-file-stream" -> "test")
      whenReady(FileRecordFactory.build(hr)) { r =>
        ingestor ! Ingest(r, AckStrategy.NoAck)
        transportProbe.expectMsg(10.seconds, Produce(FileRecord("test", "test"),
          self, AckStrategy.NoAck))
      }
    }
  }
}


class ForwardActor(to: ActorRef) extends Actor {
  def receive = {
    case x => to.forward(x)
  }
}