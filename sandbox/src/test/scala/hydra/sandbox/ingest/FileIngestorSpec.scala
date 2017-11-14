package hydra.sandbox.ingest

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import hydra.common.config.ConfigSupport
import hydra.core.ingest.HydraRequest
import hydra.core.protocol._
import hydra.core.transport.AckStrategy
import hydra.sandbox.transport.FileRecord
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.duration._

class FileIngestorSpec extends TestKit(ActorSystem("hydra-sandbox-test")) with Matchers with FunSpecLike
  with ImplicitSender with ConfigSupport with BeforeAndAfterAll with Eventually {
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
      val hr = HydraRequest(0, "test")
      ingestor ! Publish(hr)
      eventually {
        expectMsg(60.seconds, Ignore)

      }
    }

    it("joins") {
      val hr = HydraRequest(0, "test").withMetadata("hydra-file-stream" -> "test")
      ingestor ! Publish(hr)
      eventually {
        expectMsg(60.seconds, Join)
      }
    }

    it("transports") {
      val hr = HydraRequest(0, "test").withMetadata("hydra-file-stream" -> "test")
      val supervisor = TestProbe()
      ingestor ! Ingest(FileRecordFactory.build(hr).get, supervisor.ref, AckStrategy.NoAck)
      transportProbe.expectMsg(10.seconds, Produce(FileRecord("test", "test"), supervisor.ref, AckStrategy.NoAck))
    }
  }
}


class ForwardActor(to: ActorRef) extends Actor {
  def receive = {
    case x => to.forward(x)
  }
}