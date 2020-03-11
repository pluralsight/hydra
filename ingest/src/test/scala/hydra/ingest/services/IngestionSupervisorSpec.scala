package hydra.ingest.services

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestActor, TestKit, TestProbe}
import hydra.common.util.ActorUtils
import hydra.core.ingest._
import hydra.core.protocol._
import hydra.core.transport.AckStrategy
import hydra.ingest.IngestorInfo
import hydra.ingest.test.{TestRecordFactory, TimeoutRecord}
import org.joda.time.DateTime
import akka.pattern.pipe
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Created by alexsilva on 3/9/17.
  */
class IngestionSupervisorSpec
    extends TestKit(ActorSystem("hydra"))
    with Matchers
    with AnyFunSpecLike
    with ImplicitSender
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with ScalaFutures {

  override def afterAll =
    TestKit.shutdownActorSystem(
      system,
      verifySystemShutdown = true,
      duration = 10 seconds
    )

  var ingestor: TestProbe = _

  def ingestorInfo =
    IngestorInfo(
      ActorUtils.actorName(ingestor.ref),
      "global",
      ingestor.ref.path,
      DateTime.now
    )

  val except = new IllegalArgumentException

  val listener = TestProbe()

  system.eventStream.subscribe(listener.ref, classOf[IngestionTimedOut])

  override def afterEach(): Unit = {
    system.stop(ingestor.ref)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    def getPublishMsg(req: HydraRequest) = {
      val ignore = req.metadataValueEquals("ignore", "true")
      val error = req.metadataValueEquals("error", "true")
      if (ignore) Ignore else if (error) IngestorError(except) else Join
    }

    ingestor = TestProbe("ingestor")

    ingestor.setAutoPilot((sender: ActorRef, msg: Any) =>
      msg match {
        case Publish(req) =>
          sender.tell(getPublishMsg(req), ingestor.ref)
          TestActor.KeepRunning
        case Validate(req) =>
          val s = sender
          val reply = if (req.metadataValueEquals("invalid", "true")) {
            Future.successful(InvalidRequest(except))
          } else {
            TestRecordFactory.build(req).map(ValidRequest(_))
          }
          reply.pipeTo(s)(ingestor.ref)
          TestActor.KeepRunning
        case Ingest(rec, _) =>
          val timeout = rec.isInstanceOf[TimeoutRecord]
          if (!timeout) sender.tell(IngestorCompleted, ingestor.ref)
          TestActor.KeepRunning
      }
    )
  }

  val publishRequest = HydraRequest("123", "test payload")

  def ingestorRequest =
    publishRequest
      .withMetadata(
        RequestParams.HYDRA_INGESTOR_PARAM -> ActorUtils.actorName(ingestor.ref)
      )

  def ingestors =
    Seq(
      IngestorInfo(
        ActorUtils.actorName(ingestor.ref),
        "test",
        ingestor.ref.path,
        DateTime.now()
      )
    )

  describe("When supervising an ingestion") {

    it("follows the ingestion protocol") {
      val requestor = TestProbe()
      val sup = requestor.childActorOf(
        IngestionSupervisor
          .props(ingestorRequest, requestor.ref, ingestors, 1.second),
        "sup"
      )
      ingestor.expectMsg(Publish(ingestorRequest))
      ingestor.expectMsg(Validate(ingestorRequest))
      whenReady(TestRecordFactory.build(ingestorRequest))(r =>
        ingestor.expectMsg(Ingest(r, AckStrategy.NoAck))
      )

      requestor.expectMsgPF() {
        case i: IngestionReport =>
          i.statusCode shouldBe 200
      }
    }

    it("sends a Publish to the ingestor") {
      system.actorOf(
        IngestionSupervisor.props(ingestorRequest, self, ingestors, 1.second)
      )
      ingestor.expectMsgType[Publish]
      expectMsgPF() {
        case i: IngestionReport =>
          i.statusCode shouldBe 200
      }

    }

    it("reports invalid requests") {
      val requestor = TestProbe()
      val req = ingestorRequest.withMetadata("invalid" -> "true")
      requestor.childActorOf(
        IngestionSupervisor.props(req, requestor.ref, ingestors, 1.second),
        "sup"
      )
      ingestor.expectMsg(Publish(req))
      requestor.expectMsgPF() {
        case i: IngestionReport =>
          i.statusCode shouldBe 400
      }
    }

    it("times out") {
      val req = ingestorRequest.withMetadata("timeout" -> "true")
      system.actorOf(
        IngestionSupervisor.props(req, self, ingestors, 1.second),
        "sup"
      )
      ingestor.expectMsg(Publish(req))
      ingestor.expectMsg(Validate(req))
      whenReady(TestRecordFactory.build(req))(r =>
        ingestor.expectMsg(Ingest(r, AckStrategy.NoAck))
      )
      listener.expectMsgPF() {
        case IngestionTimedOut(request, time, duration, ingestors) =>
          request shouldBe req
          time.getMillis should be < DateTime.now().getMillis
          duration shouldBe 1.second
          ingestors.substring(0, 8) shouldBe "ingestor"
      }
      expectMsgPF() {
        case i: IngestionReport =>
          i.statusCode shouldBe 408
      }
    }

    it("completes with 404 when all ingestors ignore request") {
      val requestor = TestProbe()
      val req = ingestorRequest.withMetadata("ignore" -> "true")
      requestor.childActorOf(
        IngestionSupervisor.props(req, requestor.ref, ingestors, 500.millis),
        "sup"
      )
      ingestor.expectMsg(Publish(req))
      requestor.expectMsgPF() {
        case i: IngestionReport =>
          i.statusCode shouldBe 404
      }
    }

    it("completes with a 503 when ingestors error out") {
      val req = ingestorRequest.withMetadata("error" -> "true")
      val requestor = TestProbe()
      system.actorOf(
        IngestionSupervisor.props(req, requestor.ref, ingestors, 1.second),
        "sup"
      )

      requestor.expectMsgPF() {
        case i: IngestionReport =>
          i.statusCode shouldBe 503
          i.ingestors shouldBe Map(
            ActorUtils.actorName(ingestor.ref) -> IngestorError(except)
          )
      }
    }
  }
}
