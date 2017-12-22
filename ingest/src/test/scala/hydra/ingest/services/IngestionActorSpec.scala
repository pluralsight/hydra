package hydra.ingest.services

import akka.actor.{Actor, ActorSystem, Props}
import akka.cluster.pubsub.DistributedPubSub
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import hydra.core.ingest.{HydraRequest, IngestionReport, RequestParams}
import hydra.core.protocol._
import hydra.ingest.cluster.HydraRequestPublisher
import hydra.ingest.ingestors.IngestorInfo
import hydra.ingest.services.IngestorRegistry.{FindAll, FindByName, LookupResult}
import hydra.ingest.test.TestRecordFactory
import org.joda.time.DateTime
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * Created by alexsilva on 3/9/17.
  */
class IngestionActorSpec extends TestKit(ActorSystem("hydra",
  config = ConfigFactory.parseString("akka.actor.provider=cluster")
    .withFallback(ConfigFactory.load()))) with Matchers
  with FunSpecLike with ImplicitSender with Eventually with BeforeAndAfterAll {

  override def afterAll = TestKit.shutdownActorSystem(system, verifySystemShutdown = true)

  val ingestor = TestActorRef(new Actor {
    override def receive = {
      case Publish(_) => sender ! Join
      case Validate(r) => sender ! ValidRequest(TestRecordFactory.build(r).get)
      case Ingest(__, _) => sender ! IngestorCompleted
    }
  }, "test_ingestor")


  val ingestorInfo = IngestorInfo("test_ingestor", "test", ingestor.path, DateTime.now)
  val ingestionActor = system.actorOf(Props[IngestionActor])

  val registry = TestActorRef(new Actor {
    override def receive = {
      case FindByName("tester") => sender ! LookupResult(Seq(ingestorInfo))
      case FindAll => sender ! LookupResult(Seq(ingestorInfo))
    }
  }, "ingestor_registry")

  describe("The ingestion actor") {
    it("initiates an ingestion") {
      val probe = TestProbe()
      val request = HydraRequest("123", "test payload").withMetadata(RequestParams.REPLY_TO -> probe.ref.path.toString)
      ingestionActor ! request
      probe.expectMsgPF() {
        case IngestionReport(_, _, statusCode, _) =>
          statusCode shouldBe 200
      }
    }

    it("receives HydraRequest cluster pubsub events") {
      val probe = TestProbe()
      val request = HydraRequest("123", "test payload").withMetadata(RequestParams.REPLY_TO -> probe.ref.path.toString)

      val mediator = DistributedPubSub(system).mediator

      mediator ! akka.cluster.pubsub.DistributedPubSubMediator
        .Publish(HydraRequestPublisher.TopicName, request, true)

      probe.expectMsgPF() {
        case IngestionReport(_, _, statusCode, _) =>
          statusCode shouldBe 200
      }
    }

  }
}

