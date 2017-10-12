package hydra.sandbox.ingest

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import hydra.common.config.ConfigSupport
import hydra.core.ingest.HydraRequest
import hydra.core.protocol._
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

class FileIngestorSpec extends TestKit(ActorSystem("hydra-sandbox-test")) with Matchers with FunSpecLike
  with ImplicitSender with ConfigSupport with BeforeAndAfterAll {

  override def afterAll = TestKit.shutdownActorSystem(system)

  val probe = TestProbe()

  val fileProducer = system.actorOf(Props(new ForwardActor(probe.ref)), "file_producer")

  val ingestor = probe.childActorOf(Props[FileIngestor])

  describe("The FileIngestor") {
    it("ignores") {
      val hr = HydraRequest(0, "test")
      ingestor ! Publish(hr)
      expectMsg(Ignore)
    }

    it("joins") {
      val hr = HydraRequest(0, "test").withMetadata("hydra-file-stream" -> "test")
      ingestor ! Publish(hr)
      expectMsg(Join)
    }

    it("transports") {
      val hr = HydraRequest(0, "test").withMetadata("hydra-file-stream" -> "test")
      ingestor ! Ingest(FileRecordFactory.build(hr).get)
      expectMsg(IngestorCompleted)
    }
  }

}


class ForwardActor(to: ActorRef) extends Actor {
  def receive = {
    case x => to.forward(x)
  }
}