package hydra.core.transport

import java.io.File

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestActors.ForwardActor
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import hydra.common.config.ConfigSupport
import hydra.core.protocol._
import hydra.core.test.TestRecord
import hydra.core.transport.AckStrategy.{LocalAck, NoAck, TransportAck}
import hydra.core.transport.Transport.{Confirm, Deliver, TransportError}
import org.iq80.leveldb.util.FileUtils
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

class TransportSpec extends TestKit(ActorSystem("test")) with Matchers with FunSpecLike with BeforeAndAfterAll
  with ImplicitSender with ConfigSupport {

  override def afterAll() {
    super.afterAll()
    storageLocations foreach FileUtils.deleteRecursively
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  val transport = TestProbe()

  val transportManager = system.actorOf(Transport.props("tester", Props(classOf[ForwardActor], transport.ref)))

  val supervisor = TestProbe()

  private val storageLocations = List(
    new File(rootConfig.getString("akka.persistence.journal.leveldb.dir")),
    new File(rootConfig.getString("akka.persistence.snapshot-store.local.dir")))

  override def beforeAll() {
    super.beforeAll()
    storageLocations foreach FileUtils.deleteRecursively
  }

  describe("Transports") {

    it("handles NoAck produces") {
      val rec = TestRecord("OK", Some("1"), "test")
      transportManager ! Produce(rec, supervisor.ref, NoAck)
      expectMsg(RecordAccepted(supervisor.ref))
      transport.expectMsg(Deliver(rec, -1, Transport.NoAck))
    }

    it("handles LocalAck produces") {
      val rec = TestRecord("OK", Some("1"), "test")
      transportManager ! Produce(rec, supervisor.ref, LocalAck)
      transport.expectMsgPF() {
        case Deliver(r, id, ack) =>
          r shouldBe rec
          id should be > 0L
          ack shouldBe Transport.NoAck
          //simulate confirm
          transportManager ! Confirm(id)
      }

      expectMsgPF() {
        case RecordProduced(md, sup) =>
          md.deliveryId shouldBe -1
          sup shouldBe supervisor.ref
      }
    }

    it("handles TransportAck produces") {
      val rec = TestRecord("OK", Some("1"), "test")
      transportManager ! Produce(rec, supervisor.ref, TransportAck)
      transport.expectMsgPF() {
        case Deliver(r, deliveryId, ack) =>
          r shouldBe rec
          deliveryId shouldBe -1 //we don't save transport acks to the journal

          //simulate an ack from the transport
          ack.apply(Some(HydraRecordMetadata(deliveryId, System.currentTimeMillis)), None)
          transportManager ! Confirm(-1)
          expectMsgPF() { //ingestor receives this message
            case RecordProduced(md, sup) =>
              md.deliveryId shouldBe -1
              sup shouldBe supervisor.ref
          }

          //simulate an error from the transport
          ack.apply(None, Some(new IllegalArgumentException("ERROR!!")))
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