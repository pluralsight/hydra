package hydra.core.ingest

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import hydra.core.akka.ActorInitializationException
import hydra.core.akka.InitializingActor.{InitializationError, Initialized}
import hydra.core.protocol._
import hydra.core.test.{TestRecord, TestRecordFactory, TestRecordMetadata}
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
          i.error shouldBe a[ActorInitializationException]
      }

      system.actorOf(Props(classOf[TestIngestor], true, false)) ! "hello"

      expectMsg("hi!")
    }

    it("handle initialization timeouts") {
      val ct = system.actorOf(Props(classOf[TestIngestor], true, true))
      ct ! "hello"
      expectMsgPF(max = 5.seconds) {
        case i: IngestorError =>
          i.error shouldBe a[ActorInitializationException]
          i.error.asInstanceOf[ActorInitializationException].getActor.path shouldBe ct.path
          ActorInitializationException
            .unapply(i.error.asInstanceOf[ActorInitializationException]) shouldBe Some(ct,
            i.error.getMessage, i.error.getCause)
      }
    }

    it("handle errors by restarting") {
      system.actorOf(Props(classOf[TestIngestor], true, false)) ! "error"
    }

    it("calls the default init method") {
      val act = system.actorOf(Props[TestIngestorDefault])
      act ! "hello"

      expectMsg("hi!")

      act ! "timeout"
      expectMsg(1.second) //testing that override with a val won't take effect until after the constructor ends
    }


    it("handle the base ingestion protocol") {
      val sup = TestProbe()
      val ing = system.actorOf(Props(classOf[TestIngestor], true, false))
      val req = HydraRequest(1, "test")
      ing ! Publish(req)
      expectMsg(Ignore)
      ing ! Validate(req)
      expectMsg(ValidRequest(TestRecord("test-topic", Some("1"), "test")))
      ing ! RecordProduced(TestRecordMetadata(0), self)
      expectMsg(IngestorCompleted)
      ing ! RecordNotProduced(0, TestRecord("test-topic", Some("1"), "test"), new IllegalArgumentException, sup.ref)
      sup.expectMsgPF() {
        case i: IngestorError =>
          i.error shouldBe a[IllegalArgumentException]
      }

      ing ! RecordAccepted(sup.ref)
      sup.expectMsg(IngestorCompleted)
    }
  }
}

class TestIngestorDefault extends Ingestor {


  /**
    * This will _not_ override; instead it will use the default value of 1.second. We'll test it.
    */
  override val initTimeout = 2.millisecond

  val to = context.receiveTimeout

  ingest {
    case "hello" => sender ! "hi!"
    case "timeout" => sender ! to
  }

  override val recordFactory = TestRecordFactory
}

class TestIngestor(completeInit: Boolean, delayInit: Boolean) extends Ingestor {

  implicit val ec = context.dispatcher

  val err = ActorInitializationException(self, "ERROR")

  override val recordFactory = TestRecordFactory

  override def init: Future[HydraMessage] = {
    Future {
      if (delayInit) Thread.sleep((initTimeout * 2).toMillis)
      if (!completeInit) InitializationError(err) else Initialized
    }
  }

  ingest {
    case "hello" => sender ! "hi!"
    case "error" => throw new RuntimeException("ERROR!")
  }
}
