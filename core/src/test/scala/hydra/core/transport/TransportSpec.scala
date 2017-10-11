package hydra.core.transport

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import hydra.core.akka.InitializingActor.InitializationError
import hydra.core.protocol._
import hydra.core.test.TestRecord
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
      ing ! Produce(rec, ing, supervisor.ref)
      expectMsgPF() {
        case RecordNotProduced(r, err) =>
          r shouldBe rec
          err shouldBe a[IllegalStateException]
      }

      val er = rec.copy(ackStrategy = AckStrategy.Explicit)
      ing ! Produce(er, ing, supervisor.ref)
      expectMsgPF() {
        case RecordNotProduced(r, err) =>
          r shouldBe er
          err shouldBe a[IllegalStateException]
      }

      ing ! RecordProduced(null)
      expectMsg(RecordProduced(null))

      ing ! RecordNotProduced(null, new IllegalArgumentException)
      expectMsgType[RecordNotProduced[_, _]]
    }
  }
}

class TransportTester extends Transport {

  transport {
    case "hello" => sender ! "hi!"
    case "error" => throw new RuntimeException("ERROR!")
  }
}

class TransportInitErrorTester extends Transport {

  import context.dispatcher

  override def init: Future[HydraMessage] = Future(InitializationError(new IllegalArgumentException))
}
