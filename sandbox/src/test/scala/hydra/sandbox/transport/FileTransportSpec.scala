package hydra.sandbox.transport

import java.nio.file.Files

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import hydra.core.protocol.{RecordNotProduced, RecordProduced}
import hydra.core.transport.TransportSupervisor.Deliver
import hydra.core.transport.{HydraRecord, RecordMetadata, TransportCallback}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.io.Source

class FileTransportSpec extends TestKit(ActorSystem("hydra-sandbox-test")) with Matchers with FunSpecLike
  with ImplicitSender with BeforeAndAfterAll with Eventually {

  override def afterAll = TestKit.shutdownActorSystem(system, verifySystemShutdown = true, duration = 30.seconds)

  implicit override val patienceConfig = PatienceConfig(timeout = Span(60, Seconds), interval = Span(1, Seconds))

  val files = Map("hydra.transports.file.destinations.test" -> Files.createTempFile("hydra", "test").toFile)

  val transport = system.actorOf(FileTransport.props(ConfigFactory.parseMap(files.mapValues(_.getAbsolutePath).asJava)))

  val supervisor = TestProbe().ref

  val ingestor = TestProbe()

  private def callback(record: HydraRecord[_, _]): TransportCallback =
    (deliveryId: Long, md: Option[RecordMetadata], exception: Option[Throwable]) => {
      val msg = md.map(RecordProduced(_, supervisor))
        .getOrElse(RecordNotProduced(record, exception.get, supervisor))

      ingestor.ref ! msg
    }


  describe("The FileTransport") {
    it("saves to a file") {
      transport ! Deliver(FileRecord("test", "test-payload"))
      eventually(Source.fromFile(files("transports.file.destinations.test"))
        .getLines().toSeq should contain("test-payload"))
    }

    it("reports record not produced") {
      val fr = FileRecord("???", "test-payload1")
      transport.tell(Deliver(fr, 1, callback(fr)), ingestor.ref)

      ingestor.expectMsgPF(20.seconds) {
        case RecordNotProduced(r, error, sup) =>
          r shouldBe fr
          error.getClass shouldBe classOf[IllegalArgumentException]
          sup shouldBe supervisor
      }

    }

    it("saves to a file and acks the ingestor") {
      val fr = FileRecord("test", "test-payload1")
      transport.tell(Deliver(fr, 1, callback(fr)), ingestor.ref)

      ingestor.expectMsgPF(20.seconds) {
        case RecordProduced(fmd, sup) =>
          fmd shouldBe a[FileRecordMetadata]
          fmd.asInstanceOf[FileRecordMetadata].path shouldBe files("transports.file.destinations.test").getAbsolutePath
          sup shouldBe supervisor
      }

      eventually(Source.fromFile(files("transports.file.destinations.test"))
        .getLines().toSeq should contain("test-payload1"))
    }
  }
}