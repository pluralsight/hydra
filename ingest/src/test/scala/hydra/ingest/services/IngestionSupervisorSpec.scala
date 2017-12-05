package hydra.ingest.services

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestActor, TestActorRef, TestKit, TestProbe}
import hydra.common.util.ActorUtils
import hydra.core.ingest._
import hydra.core.protocol._
import hydra.core.transport.AckStrategy
import hydra.ingest.ingestors.IngestorInfo
import hydra.ingest.services.IngestorRegistry.{FindAll, FindByName, LookupResult}
import hydra.ingest.test.{TestRecordFactory, TimeoutRecord}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpecLike, Matchers}

import scala.concurrent.duration._

/**
  * Created by alexsilva on 3/9/17.
  */
class IngestionSupervisorSpec extends TestKit(ActorSystem("hydra")) with Matchers with FunSpecLike
  with ImplicitSender with BeforeAndAfterAll with BeforeAndAfterEach {

  override def afterAll = TestKit.shutdownActorSystem(system)

  var ingestor: TestProbe = _
  var registryProbe: TestProbe = _

  def ingestorInfo = IngestorInfo(ActorUtils.actorName(ingestor.ref),
    "global", ingestor.ref.path, DateTime.now)

  val except = new IllegalArgumentException

  override def afterEach(): Unit = {
    system.stop(ingestor.ref)
    system.stop(registryProbe.ref)

  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    def getPublishMsg(req: HydraRequest) = {
      val ignore = req.metadataValueEquals("ignore", "true")
      val error = req.metadataValueEquals("error", "true")
      if (ignore) Ignore else if (error) IngestorError(except) else Join
    }

    ingestor = TestProbe("ingestor")
    ingestor.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case Publish(req) =>
        sender.tell(getPublishMsg(req), ingestor.ref)
        TestActor.KeepRunning
      case Validate(req) =>
        val reply = if (req.metadataValueEquals("invalid", "true")) InvalidRequest(except) else ValidRequest(TestRecordFactory.build(req).get)
        sender.tell(reply, ingestor.ref)
        TestActor.KeepRunning
      case Ingest(rec, _) =>
        val timeout = rec.isInstanceOf[TimeoutRecord]
        if (!timeout) sender.tell(IngestorCompleted, ingestor.ref)
        TestActor.KeepRunning
    })

    registryProbe = TestProbe()
    registryProbe.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case FindByName(name) =>
        val msg = if (name == ActorUtils.actorName(ingestor.ref)) LookupResult(Seq(ingestorInfo)) else LookupResult(Seq.empty)
        sender ! msg
        TestActor.KeepRunning
      case FindAll =>
        sender ! LookupResult(Seq(ingestorInfo))
        TestActor.KeepRunning
    })
  }


  def registryActor = TestActorRef(new IngestorRegistry {
    override def receive = {
      case FindByName(name) =>
        val msg = if (name == ActorUtils.actorName(ingestor.ref)) LookupResult(Seq(ingestorInfo)) else LookupResult(Seq.empty)
        sender ! msg
      case FindAll =>
        sender ! LookupResult(Seq(ingestorInfo))
    }
  })

  val publishRequest = HydraRequest(123, "test payload")

  def ingestorRequest = publishRequest
    .withMetadata(RequestParams.HYDRA_INGESTOR_PARAM -> ActorUtils.actorName(ingestor.ref))


  describe("When supervising an ingestion") {
    it("broadcasts a request") {
      val registryProbe = TestProbe()
      system.actorOf(IngestionSupervisor.props(publishRequest, 1.second, registryProbe.ref))
      registryProbe.expectMsgType[FindAll.type]
    }

    it("looks up a target ingestor by name") {
      val registryProbe = TestProbe()
      system.actorOf(IngestionSupervisor.props(ingestorRequest, 1.second, registryProbe.ref))
      registryProbe.expectMsg(FindByName(ActorUtils.actorName(ingestor.ref)))
    }

    it("publishes to an ingestor") {
      val parent = TestProbe()
      val req = publishRequest
        .withMetadata(RequestParams.HYDRA_INGESTOR_PARAM -> ActorUtils.actorName(ingestor.ref))
      parent.childActorOf(IngestionSupervisor.props(req, 1.second, registryProbe.ref), "sup")
      registryProbe.expectMsgType[FindByName]
      ingestor.expectMsg(Publish(ingestorRequest))
    }

    it("follows the ingestion protocol") {
      val parent = TestProbe()
      val sup = parent.childActorOf(IngestionSupervisor.props(ingestorRequest, 1.second, registryProbe.ref), "sup")
      ingestor.expectMsg(Publish(ingestorRequest))
      ingestor.expectMsg(Validate(ingestorRequest))
      ingestor.expectMsg(Ingest(TestRecordFactory.build(ingestorRequest).get, AckStrategy.NoAck))
      parent.expectMsgPF() {
        case i: IngestionReport =>
          i.statusCode shouldBe 200
      }
    }

    it("sends a Publish to the ingestor") {
      system.actorOf(IngestionSupervisor.props(ingestorRequest, 1.second, registryProbe.ref))
      registryProbe.expectMsgType[FindByName]
      ingestor.expectMsgType[Publish]
    }

    it("reports invalid requests") {
      val parent = TestProbe()
      val req = ingestorRequest.withMetadata("invalid" -> "true")
      parent.childActorOf(IngestionSupervisor.props(req, 1.second, registryProbe.ref), "sup")
      registryProbe.expectMsg(FindByName(ActorUtils.actorName(ingestor.ref)))
      ingestor.expectMsg(Publish(req))
      parent.expectMsgPF() {
        case i: IngestionReport =>
          i.statusCode shouldBe 400
      }
    }

    it("times out") {
      val parent = TestProbe()
      val req = ingestorRequest.withMetadata("timeout" -> "true")
      val sup = parent.childActorOf(IngestionSupervisor.props(req, 500.millisecond, registryProbe.ref), "sup")
      registryProbe.expectMsg(FindByName(ActorUtils.actorName(ingestor.ref)))
      ingestor.expectMsg(Publish(req))
      ingestor.expectMsg(Validate(req))
      ingestor.expectMsg(Ingest(TestRecordFactory.build(req).get, AckStrategy.NoAck))
      parent.expectMsgPF() {
        case i: IngestionReport =>
          i.statusCode shouldBe 408
      }
    }

    it("completes with 404 when all ingestors ignore request") {
      val parent = TestProbe()
      val req = ingestorRequest.withMetadata("ignore" -> "true")
      parent.childActorOf(IngestionSupervisor.props(req, 500.millisecond, registryProbe.ref), "sup")
      registryProbe.expectMsg(FindByName(ActorUtils.actorName(ingestor.ref)))
      ingestor.expectMsg(Publish(req))
      parent.expectMsgPF() {
        case i: IngestionReport =>
          i.statusCode shouldBe 404
      }
    }

    it("completes with a 404 with unknown ingestors") {
      val parent = TestProbe()
      parent.childActorOf(IngestionSupervisor.props(ingestorRequest
        .withMetadata(RequestParams.HYDRA_INGESTOR_PARAM -> "unknown"), 1.second, registryProbe.ref), "sup")

      parent.expectMsgPF() {
        case i: IngestionReport =>
          i.statusCode shouldBe 404
          i.ingestors shouldBe Map.empty
      }
    }

    it("completes with a 400 when ingestors error out") {
      val req = ingestorRequest.withMetadata("error" -> "true")
      val parent = TestProbe()
      parent.childActorOf(IngestionSupervisor.props(req, 1.second, registryProbe.ref), "sup")

      parent.expectMsgPF() {
        case i: IngestionReport =>
          i.statusCode shouldBe 503
          i.ingestors shouldBe Map(ActorUtils.actorName(ingestor.ref) -> IngestorError(except))
      }
    }
  }
}