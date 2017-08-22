package hydra.core.ingest

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import hydra.core.ingest.Ingestor.{IngestorInitializationError, IngestorInitialized}
import hydra.core.protocol._
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration._

class IngestorSpec extends TestKit(ActorSystem("test")) with Matchers with FunSpecLike with BeforeAndAfterAll
  with ImplicitSender {

  override def afterAll = TestKit.shutdownActorSystem(system)

  describe("ingestors") {
    it("handle initialization") {

      system.actorOf(Props(classOf[TestIngestor], false, false)) ! "hello"

      expectMsgPF() {
        case i: IngestorError =>
          i.error shouldBe a[IngestorInitializationException]
      }

      system.actorOf(Props(classOf[TestIngestor], true, false)) ! "hello"

      expectMsg("hi!")
    }

    it("handle initialization timeouts") {
      val ct = system.actorOf(Props(classOf[TestIngestor], true, true))
      ct ! "hello"
      expectMsgPF(max = 5.seconds) {
        case i: IngestorError =>
          i.error shouldBe a[IngestorInitializationException]
          i.error.asInstanceOf[IngestorInitializationException].getActor.path shouldBe ct.path
          IngestorInitializationException
            .unapply(i.error.asInstanceOf[IngestorInitializationException]) shouldBe Some(ct,
            i.error.getMessage, i.error.getCause)
      }
    }

    it("handle errors by restarting") {
      system.actorOf(Props(classOf[TestIngestor], true, false)) ! "error"
    }

    it("handle the base ingestion protocol") {
      val ing = system.actorOf(Props(classOf[TestIngestor], true, false))
      val req = HydraRequest(1, "test")
      ing ! Publish(req)
      expectMsg(Ignore)
      ing ! Validate(req)
      expectMsg(ValidRequest)
      ing ! ProducerAck(self, None)
      expectMsg(IngestorCompleted)
      ing ! ProducerAck(self, Some(new IllegalArgumentException))
      expectMsgPF() {
        case i: IngestorError =>
          i.error shouldBe a[IllegalArgumentException]
      }
    }
  }
}

class TestIngestor(completeInit: Boolean, delayInit: Boolean) extends Ingestor {

  implicit val ec = context.dispatcher

  val err = IngestorInitializationException(self, "ERROR")

  override def initIngestor: Future[HydraMessage] = {

    Future {
      if (delayInit) Thread.sleep((initTimeout * 2).toMillis)
      if (!completeInit) IngestorInitializationError(err) else IngestorInitialized
    }
  }

  ingest {
    case "hello" => sender ! "hi!"
    case "error" => throw new RuntimeException("ERROR!")
  }
}
