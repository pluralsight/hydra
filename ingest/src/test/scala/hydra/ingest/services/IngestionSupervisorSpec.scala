package hydra.ingest.services

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestActor, TestActorRef, TestKit, TestProbe}
import hydra.core.ingest.{HydraRequest, RequestParams}
import hydra.core.protocol.{Join, Publish, ValidRequest, Validate}
import hydra.ingest.ingestors.IngestorInfo
import hydra.ingest.services.IngestorRegistry.{FindAll, FindByName, LookupResult}
import org.joda.time.DateTime
import org.scalatest.{FunSpecLike, Matchers}

import scala.concurrent.duration._

/**
  * Created by alexsilva on 3/9/17.
  */
class IngestionSupervisorSpec extends TestKit(ActorSystem("hydra")) with Matchers with FunSpecLike with ImplicitSender {

  val ingestor = TestProbe()
  ingestor.setAutoPilot(new TestActor.AutoPilot {
    def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
      msg match {
        case Publish(_) => sender ! Join; TestActor.KeepRunning
        case Validate(request) => sender ! ValidRequest; TestActor.KeepRunning
      }
  })

  val registry = TestProbe()
  registry.setAutoPilot(new TestActor.AutoPilot {
    def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
      msg match {
        case p@Publish(_) => ingestor.ref.tell(p, sender); TestActor.KeepRunning
        case FindByName(_) => sender ! ingestor; TestActor.KeepRunning
        case FindAll =>
          sender ! LookupResult(Seq(IngestorInfo(ingestor.ref.path.name, "test", ingestor.ref.path, DateTime.now())))
          TestActor.KeepRunning
      }
  })

  val request = HydraRequest(123, "test payload")

  val ingestorRequest = request.withMetadata(RequestParams.HYDRA_INGESTOR_PARAM -> "test_ingestor")

  describe("When supervising an ingestion") {
    it("broadcasts a request") {
      val ingestionSupervisor = TestActorRef(IngestionSupervisor.props(request, 1.second, registry.ref))
      registry.expectMsgType[FindAll.type ]
    }

    it("looks up a target ingestor instead of publishing") {
      val ingestionSupervisor = TestActorRef(IngestionSupervisor.props(ingestorRequest, 1.second, registry.ref))
      registry.expectMsgType[FindByName]
      expectMsgType[Validate]
    }

    it("sends a validate message to the ingestor") {
      val ingestionSupervisor = TestActorRef(IngestionSupervisor.props(request, 1.second, registry.ref))
      ingestor.expectMsgType[Publish]
     // ingestor.expectMsgType[Validate]
    }

  }

}
