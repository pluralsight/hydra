package hydra.ingest.services

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import hydra.core.ingest.{HydraRequest, IngestionReport, RequestParams}
import hydra.core.protocol._
import hydra.ingest.ingestors.IngestorInfo
import hydra.ingest.services.IngestorRegistry.{FindAll, FindByName, LookupResult}
import hydra.ingest.test.TestRecordFactory
import org.joda.time.DateTime
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.duration._
/**
  * Created by alexsilva on 3/9/17.
  */
class IngestionActorSpec extends TestKit(ActorSystem("hydra")) with Matchers
  with FunSpecLike with ImplicitSender with Eventually with BeforeAndAfterAll {

  override def afterAll = TestKit.shutdownActorSystem(system, verifySystemShutdown = true, duration = 10 seconds)


  val ingestor = TestActorRef(new Actor {
    override def receive = {
      case Publish(_) => sender ! Join
      case Validate(r) => sender ! ValidRequest(TestRecordFactory.build(r).get)
      case Ingest(__, _) => sender ! IngestorCompleted
    }
  }, "test_ingestor")


  val ingestorInfo = IngestorInfo("test_ingestor", "test", ingestor.path, DateTime.now)

  describe("The ingestion actor") {
    it("initiates an ingestion") {
      val registry = TestActorRef(new Actor {
        override def receive = {
          case FindByName("tester") => sender ! LookupResult(Seq(ingestorInfo))
          case FindAll => sender ! LookupResult(Seq(ingestorInfo))
        }
      }, "ingestor_registry")
      val probe = TestProbe()
      val ingestionActor = system.actorOf(Props[IngestionActor])
      val request = HydraRequest(123, "test payload").withMetadata(RequestParams.REPLY_TO -> probe.ref.path.toString)
      ingestionActor ! request
      probe.expectMsgPF() {
        case IngestionReport(_, _, statusCode, _) =>
          statusCode shouldBe 200
      }

      system.stop(registry)
      system.stop(ingestionActor)
    }

    it("errors an ingestion if it can't find the registry") {
      val ingestionActor = system.actorOf(Props[IngestionActor])
      val probe = TestProbe()
      val request = HydraRequest(123, "test payload").withMetadata(RequestParams.REPLY_TO -> probe.ref.path.toString)
      ingestionActor ! request
      expectMsgType[IngestionError]
      system.stop(ingestionActor)
    }

  }
}

