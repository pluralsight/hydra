package hydra.sandbox.transport

import java.nio.file.Files

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import hydra.core.protocol.{Produce, ProduceOnly, RecordNotProduced, RecordProduced}
import hydra.core.transport.AckStrategy
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.duration._
import scala.io.Source

class FileTransportSpec extends TestKit(ActorSystem("hydra-sandbox-test")) with Matchers with FunSpecLike
  with ImplicitSender with BeforeAndAfterAll with Eventually {

  override def afterAll = TestKit.shutdownActorSystem(system, verifySystemShutdown = true)

  implicit override val patienceConfig = PatienceConfig(timeout = Span(60, Seconds), interval = Span(1, Seconds))

  val files = Map("test" -> Files.createTempFile("hydra", "test").toFile)

  val transport = system.actorOf(FileTransport.props(files.mapValues(_.getAbsolutePath)))

  describe("The FileTransport") {
    it("saves to a file") {
      transport ! ProduceOnly(FileRecord("test", "test-payload"))
      eventually(Source.fromFile(files("test")).getLines().toSeq should contain("test-payload"))
    }

    it("reports record not produced") {
      val fr = FileRecord("???", "test-payload1").copy(ackStrategy = AckStrategy.Explicit)
      val ingestor = TestProbe()
      val supervisor = TestProbe()
      transport ! Produce(fr, ingestor.ref, supervisor.ref)

      eventually {
        ingestor.expectMsgPF(20.seconds) {
          case RecordNotProduced(r, error, sup) =>
            r shouldBe fr
            error.getClass shouldBe classOf[IllegalArgumentException]
            sup.get shouldBe supervisor.ref
        }
      }
    }

    it("saves to a file and acks the ingestor") {
      val ingestor = TestProbe()
      val supervisor = TestProbe()
      val fr = FileRecord("test", "test-payload1").copy(ackStrategy = AckStrategy.Explicit)
      transport ! Produce(fr, ingestor.ref, supervisor.ref)

      ingestor.expectMsg(20.seconds, RecordProduced(FileRecordMetadata(files("test").getAbsolutePath,
        0), Some(supervisor.ref)))

      eventually(Source.fromFile(files("test")).getLines().toSeq should contain("test-payload1"))
    }
  }

}
