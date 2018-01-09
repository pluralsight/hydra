package hydra.core.transport

import java.io.File

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestActors.ForwardActor
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import hydra.common.config.ConfigSupport
import hydra.core.protocol._
import hydra.core.test.TestRecord
import hydra.core.transport.AckStrategy.{Persisted, NoAck, Replicated}
import hydra.core.transport.TransportSupervisor.{Confirm, Deliver, TransportError}
import org.iq80.leveldb.util.FileUtils
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

class TransportSupervisorSpec extends TestKit(ActorSystem("test")) with Matchers with FunSpecLike with BeforeAndAfterAll
  with ImplicitSender with ConfigSupport {

  override def afterAll() {
    super.afterAll()
    storageLocations foreach FileUtils.deleteRecursively
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  val transport = TestProbe()

  val transportManager = system.actorOf(TransportSupervisor.props("tester", Props(classOf[ForwardActor], transport.ref)))

  val supervisor = TestProbe()

  private val storageLocations = List(
    new File(rootConfig.getString("akka.persistence.journal.leveldb.dir")),
    new File(rootConfig.getString("akka.persistence.snapshot-store.local.dir")))

  override def beforeAll() {
    super.beforeAll()
    storageLocations foreach FileUtils.deleteRecursively
  }

  describe("Transport supervisors") {

    it("forwards Deliver messages") {
      val rec = TestRecord("OK", Some("1"), "test")
      transportManager ! Deliver(rec)
      transport.expectMsg(Deliver(rec))
    }

    it("handles NoAck produces") {
      val rec = TestRecord("OK", Some("1"), "test")
      transportManager ! Produce(rec, supervisor.ref, NoAck)
      expectMsg(RecordAccepted(supervisor.ref))
      transport.expectMsg(Deliver(rec, -1, NoCallback))
    }

    it("handles LocalAck produces") {
      val rec = TestRecord("OK", Some("1"), "test")
      transportManager ! Produce(rec, supervisor.ref, Persisted)
      transport.expectMsgPF() {
        case Deliver(r, id, ack) =>
          r shouldBe rec
          id should be > 0L
          ack shouldBe a[TransportSupervisorCallback]
          //simulate confirm
          transportManager ! Confirm(id)
      }

      expectMsgPF() {
        case RecordProduced(md, sup) =>
          md shouldBe a[HydraRecordMetadata]
          sup shouldBe supervisor.ref
      }
    }

    it("handles TransportAck produces") {
      val rec = TestRecord("OK", Some("1"), "test")
      transportManager ! Produce(rec, supervisor.ref, Replicated)
      transport.expectMsgPF() {
        case Deliver(r, deliveryId, callback) =>
          r shouldBe rec
          deliveryId shouldBe -1 //we don't save transport acks to the journal

          //simulate an callback from the transport
          callback.onCompletion(-1, Some(HydraRecordMetadata(System.currentTimeMillis)), None)
          transportManager ! Confirm(-1)
          expectMsgPF() { //ingestor receives this message
            case RecordProduced(_, sup) =>
              sup shouldBe supervisor.ref
          }

          //simulate an error from the transport
          callback.onCompletion(-1, None, Some(new IllegalArgumentException("ERROR!!")))
          transportManager ! TransportError(-1)
          expectMsgPF() { //ingestor receives this message
            case RecordNotProduced(r, e, s) =>
              r shouldBe rec
              e shouldBe an[IllegalArgumentException]
              e.getMessage shouldBe "ERROR!!"
              s shouldBe supervisor.ref
          }
      }
    }
  }
}