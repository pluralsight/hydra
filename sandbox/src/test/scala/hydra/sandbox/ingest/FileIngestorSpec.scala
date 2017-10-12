package hydra.sandbox.ingest

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import hydra.common.config.ConfigSupport
import hydra.core.ingest.HydraRequest
import hydra.core.protocol._
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}
import scala.concurrent.duration._

class FileIngestorSpec extends TestKit(ActorSystem("hydra-sandbox-test")) with Matchers with FunSpecLike
  with ImplicitSender with ConfigSupport with BeforeAndAfterAll {
  val probe = TestProbe()

  val fileProducer = system.actorOf(Props(new ForwardActor(probe.ref)), "file_producer")

  val ingestor = probe.childActorOf(Props[FileIngestor])

  override def afterAll = {
    system.stop(ingestor)
    TestKit.shutdownActorSystem(system)
  }

  describe("The FileIngestor") {
    it("ignores") {
      val hr = HydraRequest(0, "test")
      ingestor ! Publish(hr)
      expectMsg(10.seconds, Ignore)
    }

    it("joins") {
      val hr = HydraRequest(0, "test").withMetadata("hydra-file-stream" -> "test")
      ingestor ! Publish(hr)
      expectMsg(10.seconds, Join)
    }

    it("transports") {
      val hr = HydraRequest(0, "test").withMetadata("hydra-file-stream" -> "test")
      ingestor ! Ingest(FileRecordFactory.build(hr).get)
      expectMsg(10.seconds, IngestorCompleted)
    }
  }

}


class ForwardActor(to: ActorRef) extends Actor {
  def receive = {
    case x => to.forward(x)
  }
}