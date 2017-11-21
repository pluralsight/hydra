package hydra.sandbox.transport

import java.nio.file.Files

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import hydra.core.protocol.{Produce, RecordNotProduced, RecordProduced}
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

  val supervisor = TestProbe().ref

  describe("The FileTransport") {
    it("saves to a file") {
      transport ! Produce(FileRecord("test", "test-payload"), supervisor, AckStrategy.NoAck)
      eventually(Source.fromFile(files("test")).getLines().toSeq should contain("test-payload"))
    }

    it("reports record not produced") {
      val fr = FileRecord("???", "test-payload1")
      val ingestor = TestProbe()
      val supervisor = TestProbe()
      transport.tell(Produce(fr, supervisor.ref, AckStrategy.TransportAck), ingestor.ref)

      eventually {
        ingestor.expectMsgPF(20.seconds) {
          case RecordNotProduced(r, error, sup) =>
            r shouldBe fr
            error.getClass shouldBe classOf[IllegalArgumentException]
            sup shouldBe supervisor.ref
        }
      }
    }

    it("saves to a file and acks the ingestor") {
      val ingestor = TestProbe()
      val supervisor = TestProbe()
      val fr = FileRecord("test", "test-payload1")
      transport.tell(Produce(fr, supervisor.ref, AckStrategy.TransportAck), ingestor.ref)

      ingestor.expectMsgPF(20.seconds) { case RecordProduced(fmd, sup) =>
        fmd shouldBe a[FileRecordMetadata]
        fmd.asInstanceOf[FileRecordMetadata].path shouldBe files("test").getAbsolutePath
        fmd.deliveryId shouldBe 0
        sup shouldBe supervisor.ref
      }

      eventually(Source.fromFile(files("test")).getLines().toSeq should contain("test-payload1"))
    }
  }

}