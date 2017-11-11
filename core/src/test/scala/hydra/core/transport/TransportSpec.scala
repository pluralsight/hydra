package hydra.core.transport

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import hydra.core.akka.InitializingActor.InitializationError
import hydra.core.protocol._
import hydra.core.test.TestRecord
import hydra.core.transport.AckStrategy.NoAck
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.Future

class TransportSpec extends TestKit(ActorSystem("test")) with Matchers with FunSpecLike with BeforeAndAfterAll
  with ImplicitSender {

  override def afterAll = TestKit.shutdownActorSystem(system)

  describe("Transports") {
    it("handles initialization") {

      system.actorOf(Props(classOf[TransportTester])) ! "hello"

      expectMsg("hi!")
    }

    it("reply with init error if didn't initialize") {
      val ing = system.actorOf(Props(classOf[TransportInitErrorTester]))
      ing ! "hello"
      expectMsgPF() {
        case InitializationError(e) => e shouldBe a[IllegalArgumentException]
      }
    }

    it("handle the base transport protocol") {
      val ing = system.actorOf(Props(classOf[TransportTester]))
      val rec = TestRecord("test", Some("1"), "test")
      val supervisor = TestProbe()
      ing ! Produce(rec, supervisor.ref, NoAck)
      expectMsgPF() {
        case RecordNotProduced(deliveryId, r, err, sup) =>
          deliveryId shouldBe -1 //this is coming from the base Transport class
          r shouldBe rec
          err shouldBe a[IllegalStateException]
          sup shouldBe supervisor.ref
      }

      ing ! Produce(rec, supervisor.ref, AckStrategy.TransportAck)
      expectMsgPF() {
        case RecordNotProduced(deliveryId, r, err, _) =>
          r shouldBe rec
          deliveryId shouldBe -1 //coming from base transport class
          err shouldBe a[IllegalStateException]
      }

      ing ! RecordProduced(null, supervisor.ref)
      expectMsg(RecordProduced(null, supervisor.ref))

      ing ! RecordNotProduced(133, null, new IllegalArgumentException, null)
      expectMsgType[RecordNotProduced[_, _]]
    }
  }
}

class TransportTester extends Transport {

  transport {
    case "hello" => sender ! "hi!"
    case "error" => throw new RuntimeException("ERROR!")
    case Produce(req, sup, ack) if (req.payload == "test-produce") =>
      sender ! RecordProduced(SimpleRecordMetadata(0), sup)

    case Produce(req, sup, ack) if (req.payload == "test-error-produce") =>
      sender ! RecordNotProduced(-2, req, new IllegalArgumentException("test-error-produce"), sup)
  }
}

class TransportInitErrorTester extends Transport {

  import context.dispatcher

  override def init: Future[HydraMessage] = Future(InitializationError(new IllegalArgumentException))
}
