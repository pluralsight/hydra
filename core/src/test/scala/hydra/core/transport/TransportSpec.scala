package hydra.core.transport

import java.io.File

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import hydra.common.config.ConfigSupport
import hydra.core.protocol._
import hydra.core.test.TestRecord
import hydra.core.transport.AckStrategy.{NoAck, Persisted, Replicated}
import hydra.core.transport.Transport.{Confirm, Deliver, TransportError}
import org.iq80.leveldb.util.FileUtils
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.BeforeAndAfterAll

class TransportSpec
    extends TestKit(ActorSystem("TransportSupervisorSpec"))
    with Matchers
    with AnyFunSpecLike
    with BeforeAndAfterAll
    with ImplicitSender
    with ConfigSupport {

  override def afterAll() {
    super.afterAll()
    storageLocations foreach FileUtils.deleteRecursively
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  val transportProbe = TestProbe()

  val transport = system.actorOf(Props(new TestTransport(transportProbe.ref)))

  val supervisor = TestProbe()

  private val storageLocations = List(
    new File(rootConfig.getString("akka.persistence.journal.leveldb.dir")),
    new File(rootConfig.getString("akka.persistence.snapshot-store.local.dir"))
  )

  override def beforeAll() {
    super.beforeAll()
    storageLocations foreach FileUtils.deleteRecursively
  }

  describe("The Transport trait") {

    it("handles NoAck produces") {
      val rec = TestRecord("OK", "1", "test", NoAck)
      transport ! Produce(rec, supervisor.ref, NoAck)
      expectMsg(RecordAccepted(supervisor.ref, "OK"))
      transportProbe.expectMsg(Deliver(rec, -1, NoCallback))
    }

    it("handles LocalAck produces") {
      val rec = TestRecord("OK", "1", "test", Persisted)
      transport ! Produce(rec, supervisor.ref, Persisted)
      transportProbe.expectMsgPF() {
        case Deliver(r, id, ack) =>
          r shouldBe rec
          id should be > 0L
          ack shouldBe a[TransportSupervisorCallback]
          //simulate confirm
          transport ! Confirm(id)
      }

      expectMsgPF() {
        case RecordProduced(md, sup) =>
          md shouldBe a[HydraRecordMetadata]
          sup shouldBe supervisor.ref
      }
    }

    it("handles TransportAck produces") {
      val rec = TestRecord("OK", "1", "test", Replicated)
      transport ! Produce(rec, supervisor.ref, Replicated)
      transportProbe.expectMsgPF() {
        case Deliver(r, deliveryId, callback) =>
          r shouldBe rec
          deliveryId shouldBe -1 //we don't save transport acks to the journal

          //simulate an callback from the transport
          callback.onCompletion(
            -1,
            Some(HydraRecordMetadata(System.currentTimeMillis, "", Replicated)),
            None
          )
          transport ! Confirm(-1)
          expectMsgPF() { //ingestor receives this message
            case RecordProduced(_, sup) =>
              sup shouldBe supervisor.ref
          }

          //simulate an error from the transport
          callback.onCompletion(
            -1,
            None,
            Some(new IllegalArgumentException("ERROR!!"))
          )
          transport ! TransportError(-1)
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

class TestTransport(forwardActor: ActorRef) extends Transport {

  override def transport: Receive = {
    case d @ Deliver(_, _, _) =>
      forwardActor ! d
  }
}
