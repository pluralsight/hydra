package hydra.ingest.services

import akka.actor.{Actor, ActorInitializationException, ActorSystem}
import akka.pattern.pipe
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import hydra.common.util.ActorUtils
import hydra.core.ingest.{HydraRequest, IngestionReport, RequestParams}
import hydra.core.protocol._
import hydra.ingest.IngestorInfo
import hydra.ingest.services.IngestorRegistry.{
  FindAll,
  FindByName,
  LookupResult
}
import hydra.ingest.test.{TestRecordFactory, TimeoutRecord}
import org.joda.time.DateTime
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by alexsilva on 3/9/17.
  */
class DefaultIngestionHandlerSpec
    extends TestKit(ActorSystem("hydra"))
    with Matchers
    with AnyFunSpecLike
    with ImplicitSender
    with Eventually
    with BeforeAndAfterAll {

  override def afterAll =
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)

  val ingestor = TestActorRef(
    new Actor {

      override def receive = {
        case Publish(_) => sender ! Join
        case Validate(r) =>
          TestRecordFactory.build(r).map(ValidRequest(_)) pipeTo sender
        case Ingest(rec, _) =>
          val timeout = rec.isInstanceOf[TimeoutRecord]
          sender ! (if (!timeout) IngestorCompleted)
      }
    },
    "test_ingestor"
  )

  val ingestorInfo =
    IngestorInfo("test_ingestor", "test", ingestor.path, DateTime.now)

  val registry = TestActorRef(
    new Actor {

      override def receive = {
        case FindByName("tester") => sender ! LookupResult(Seq(ingestorInfo))
        case FindByName("fail") =>
          sender ! LookupResult(
            Seq(IngestorInfo("fail", "f", null, DateTime.now))
          ) //force a NPE
        case FindByName(_) => sender ! LookupResult(Nil)
        case FindAll       => sender ! LookupResult(Seq(ingestorInfo))
      }
    },
    "ingestor_registry"
  )

  describe("The default ingestion handler actor") {
    it("completes an ingestion") {
      val request = HydraRequest("123", "test payload")
      system.actorOf(DefaultIngestionHandler.props(request, registry, self))
      expectMsgPF() {
        case IngestionReport(_, _, statusCode) =>
          statusCode shouldBe 200
      }
    }

    it("fails a request") {
      val request = HydraRequest("123", "fail")
        .withMetadata(RequestParams.HYDRA_INGESTOR_PARAM -> "fail")
      system.actorOf(DefaultIngestionHandler.props(request, registry, self))
      expectMsgPF() {
        case HydraApplicationError(e) =>
          e shouldBe an[ActorInitializationException]
      }
    }

    it("broadcasts a request") {
      val registryProbe = TestProbe()
      val request = HydraRequest("123", "test payload")
      system.actorOf(
        DefaultIngestionHandler.props(request, registryProbe.ref, self)
      )
      registryProbe.expectMsgType[FindAll.type]
    }

    it("looks up a target ingestor by name") {
      val registryProbe = TestProbe()
      val request = HydraRequest("123", "test payload")
        .withMetadata(
          RequestParams.HYDRA_INGESTOR_PARAM -> ActorUtils.actorName(ingestor)
        )
      system.actorOf(
        DefaultIngestionHandler.props(request, registryProbe.ref, self)
      )
      registryProbe.expectMsg(FindByName(ActorUtils.actorName(ingestor)))
    }

    it("publishes to an ingestor") {
      val registryProbe = TestProbe()
      val request = HydraRequest("123", "test payload")
        .withMetadata(
          RequestParams.HYDRA_INGESTOR_PARAM -> ActorUtils.actorName(ingestor)
        )
      system.actorOf(
        DefaultIngestionHandler.props(request, registryProbe.ref, self)
      )
      registryProbe.expectMsgType[FindByName]
    }

    it("completes with a 404 with unknown ingestors") {
      val parent = TestProbe()
      val request = HydraRequest("123", "test payload")
        .withMetadata(RequestParams.HYDRA_INGESTOR_PARAM -> "unknown")
      val props = DefaultIngestionHandler.props(request, registry, parent.ref)
      system.actorOf(props, "sup")

      parent.expectMsgPF() {
        case i: IngestionReport =>
          i.statusCode shouldBe 404
          i.ingestors shouldBe Map.empty
      }
    }

    it("times out") {
      import scala.concurrent.duration._
      val request =
        HydraRequest("123", "test payload").withMetadata("timeout" -> "true")
      val props =
        DefaultIngestionHandler.props(request, registry, self, 500.millis)
      system.actorOf(props)
      expectMsgPF() {
        case i: IngestionReport =>
          i.statusCode shouldBe 408
      }
    }
    //    it("receives HydraRequest cluster pubsub events") {
    //      val request = HydraRequest("123", "test payload")
    //      val mediator = DistributedPubSub(system).mediator
    //
    //      mediator ! akka.cluster.pubsub.DistributedPubSubMediator
    //        .Publish(HydraRequestPublisher.TopicName, request, true)
    //
    //      expectMsgPF() {
    //        case IngestionReport(_, _, statusCode) =>
    //          statusCode shouldBe 200
    //      }
    //    }

  }
}
