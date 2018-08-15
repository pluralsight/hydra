package hydra.core.ingest

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.pluralsight.hydra.reflect.DoNotScan
import hydra.core.akka.ActorInitializationException
import hydra.core.akka.InitializingActor.{InitializationError, Initialized}
import hydra.core.protocol._
import hydra.core.test.{TestRecord, TestRecordFactory, TestRecordMetadata}
import hydra.core.transport.AckStrategy
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class IngestorSpec extends TestKit(ActorSystem("test")) with Matchers with FunSpecLike with BeforeAndAfterAll
  with ImplicitSender {

  override def afterAll = TestKit.shutdownActorSystem(system)

  describe("ingestors") {
    it("handle initialization") {

      system.actorOf(Props(classOf[TestIngestor], false, false)) ! "hello"

      expectMsgPF() {
        case i: IngestorError =>
          i.cause shouldBe a[ActorInitializationException]
      }

      system.actorOf(Props(classOf[TestIngestor], true, false)) ! "hello"

      expectMsg("hi!")
    }

    it("handle initialization timeouts") {
      val ct = system.actorOf(Props(classOf[TestIngestor], true, true))
      ct ! "hello"
      expectMsgPF(max = 5.seconds) {
        case i: IngestorError =>
          i.cause shouldBe a[ActorInitializationException]
          i.cause.asInstanceOf[ActorInitializationException].getActor.path shouldBe ct.path
          ActorInitializationException
            .unapply(i.cause.asInstanceOf[ActorInitializationException]) shouldBe Some(ct,
            i.cause.getMessage, i.cause.getCause)
      }
    }

    it("handle errors by restarting") {
      system.actorOf(Props(classOf[TestIngestor], true, false)) ! "error"
    }

    it("allows for custom validation") {
      val ing = system.actorOf(Props(classOf[TestIngestor], true, false))
      ing ! Validate(HydraRequest("1", "test").withMetadata("invalid" -> "true"))
      expectMsgType[InvalidRequest]
      ing ! Validate(HydraRequest("1", "test"))
      expectMsg(ValidRequest(TestRecord("test-topic", Some("1"), "test", AckStrategy.NoAck)))
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
      val req = HydraRequest("1", "test")
      ing ! Publish(req)
      expectMsg(Ignore)
      ing ! Validate(req)
      expectMsg(ValidRequest(TestRecord("test-topic", Some("1"), "test", AckStrategy.NoAck)))
      ing ! RecordProduced(TestRecordMetadata(0, 0, "", AckStrategy.NoAck), self)
      expectMsg(IngestorCompleted)
      ing ! RecordNotProduced(TestRecord("test-topic", Some("1"), "test", AckStrategy.NoAck), new IllegalArgumentException, sup.ref)
      sup.expectMsgPF() {
        case i: IngestorError =>
          i.cause shouldBe a[IllegalArgumentException]
      }

      ing ! RecordAccepted(sup.ref, "")
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

@DoNotScan
class TestIngestor(completeInit: Boolean, delayInit: Boolean) extends Ingestor {

  implicit val ec = context.dispatcher

  override def initTimeout: FiniteDuration = 2.seconds

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

  override def validateRequest(request: HydraRequest): Try[HydraRequest] = {
    if (request.hasMetadata("invalid")) Failure(new IllegalArgumentException) else Success(request)
  }
}
