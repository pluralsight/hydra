package hydra.core.ingest

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.pluralsight.hydra.reflect.DoNotScan
import hydra.core.akka.ActorInitializationException
import hydra.core.akka.InitializingActor.{InitializationError, Initialized}
import hydra.core.protocol._
import hydra.core.test.{TestIngestorDefault, TestRecord, TestRecordFactory, TestRecordMetadata}
import hydra.core.transport.AckStrategy
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class IngestorSpec
    extends TestKit(ActorSystem("test"))
    with Matchers
    with AnyFunSpecLike
    with BeforeAndAfterAll
    with ImplicitSender {

  override def afterAll = TestKit.shutdownActorSystem(system)

  describe("ingestors") {

    it("handles delayed inits") {
      //init takes longer than timeout
      val ct = system.actorOf(Props(classOf[TestIngestor], true, true))
      ct ! "hello"
      expectMsg(max = 5 seconds, "hi!")
    }

    it("retries errors on init") {
      //init errors first time but then succeeds
      val ct = system.actorOf(Props(classOf[SucceedAfterRetryIngestor], 2))
      ct ! "test"
      expectMsg("test")
    }

    it("allows for custom validation") {
      val ing = system.actorOf(Props(classOf[TestIngestor], true, false))
      ing ! Validate(
        HydraRequest("1", "test").withMetadata("invalid" -> "true")
      )
      expectMsgType[InvalidRequest]
      ing ! Validate(HydraRequest("1", "test"))
      expectMsg(
        ValidRequest(TestRecord("test-topic", "1", "test", AckStrategy.NoAck))
      )
    }

    it("calls the default init method") {
      val act = system.actorOf(Props[TestIngestorDefault])
      act ! "hello"

      expectMsg("hi!")

      act ! "timeout"
      expectMsg(
        1.second
      ) //testing that override with a val won't take effect until after the constructor ends
    }

    it("handle the base ingestion protocol") {
      val sup = TestProbe()
      val ing = system.actorOf(Props(classOf[TestIngestor], true, false))
      val req = HydraRequest("1", "test")
      ing ! Publish(req)
      expectMsg(Ignore)
      ing ! Validate(req)
      expectMsg(
        ValidRequest(TestRecord("test-topic", "1", "test", AckStrategy.NoAck))
      )
      ing ! RecordProduced(
        TestRecordMetadata(0, 0, "", AckStrategy.NoAck),
        self
      )
      expectMsg(IngestorCompleted)
      ing ! RecordNotProduced(
        TestRecord("test-topic", "1", "test", AckStrategy.NoAck),
        new IllegalArgumentException,
        sup.ref
      )
      sup.expectMsgPF() {
        case i: IngestorError =>
          i.cause shouldBe a[IllegalArgumentException]
      }

      ing ! RecordAccepted(sup.ref, "")
      sup.expectMsg(IngestorCompleted)
    }
  }
}

@DoNotScan
class TestIngestor(completeInit: Boolean, delayInit: Boolean) extends Ingestor {

  implicit val ec = context.dispatcher

  override def initTimeout: FiniteDuration = 1.seconds

  val err = ActorInitializationException(self, "ERROR")

  override val recordFactory = TestRecordFactory

  override def init: Future[HydraMessage] = {
    log.debug("Trying init")
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
    if (request.hasMetadata("invalid")) Failure(new IllegalArgumentException)
    else Success(request)
  }
}

@DoNotScan
class SucceedAfterRetryIngestor(retries: Int) extends Ingestor {

  implicit val ec = context.dispatcher

  override def initTimeout: FiniteDuration = 1.seconds

  val err = ActorInitializationException(self, "ERROR")

  override val recordFactory = TestRecordFactory

  private var retryNo = 1

  override def init: Future[HydraMessage] = {
    retryNo += 1
    Future {
      if (retryNo < retries) InitializationError(err) else Initialized
    }
  }

  ingest {
    case msg => sender ! msg
  }

  override def validateRequest(request: HydraRequest): Try[HydraRequest] = {
    if (request.hasMetadata("invalid")) Failure(new IllegalArgumentException)
    else Success(request)
  }
}
