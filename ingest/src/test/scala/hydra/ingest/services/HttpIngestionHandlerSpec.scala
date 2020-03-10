package hydra.ingest.services

import akka.actor.{Actor, ActorInitializationException, ActorSystem}
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.pattern.pipe
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import hydra.core.http.{HydraDirectives, ImperativeRequestContext}
import hydra.core.ingest.{HydraRequest, IngestionReport, RequestParams}
import hydra.core.protocol._
import hydra.ingest.IngestorInfo
import hydra.ingest.services.IngestorRegistry.{
  FindAll,
  FindByName,
  LookupResult
}
import hydra.ingest.test.TestRecordFactory
import org.joda.time.DateTime
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * Created by alexsilva on 3/9/17.
  */
class HttpIngestionHandlerSpec
    extends TestKit(ActorSystem("hydra"))
    with Matchers
    with AnyFunSpecLike
    with ImplicitSender
    with BeforeAndAfterAll
    with HydraDirectives
    with Eventually {

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(1000 millis),
    interval = scaled(100 millis)
  )

  override def afterAll =
    TestKit.shutdownActorSystem(
      system,
      verifySystemShutdown = true,
      duration = 10 seconds
    )

  val ingestor = TestActorRef(
    new Actor {

      override def receive = {
        case Publish(_) => sender ! Join
        case Validate(r) =>
          if (r.correlationId == "400") {
            sender ! InvalidRequest(new IllegalArgumentException)
          } else {
            TestRecordFactory.build(r).map(ValidRequest(_)) pipeTo sender
          }
        case Ingest(r, _) => sender ! IngestorCompleted
      }
    },
    "test_ingestor"
  )

  val ingestorInfo =
    IngestorInfo("test_ingestor", "test", ingestor.path, DateTime.now)

  val registry = TestActorRef(
    new Actor {

      override def receive = {
        case FindByName("invalid_ingestor") =>
          sender ! LookupResult(
            Seq(IngestorInfo("invalid_ingestor", "test", null, DateTime.now))
          )
        case FindByName("tester") =>
          sender ! LookupResult(Seq(ingestorInfo))
        case FindAll =>
          sender ! LookupResult(Seq(ingestorInfo))
      }
    },
    "ingestor_registry"
  )

  describe("When starting an HTTP ingestion") {
    it("completes the request with 400") {
      val ctx = new ImperativeRequestContext {
        var completed: ToResponseMarshallable = _
        var error: Throwable = _

        override def complete(obj: ToResponseMarshallable): Unit =
          completed = obj

        override def failWith(error: Throwable): Unit = this.error = error
      }
      val req = HydraRequest("400", "test payload")

      system.actorOf(HttpIngestionHandler.props(req, 3.seconds, ctx, registry))
      eventually {
        ctx.completed should not be null
      }
      ctx.completed.value.asInstanceOf[(_, _)]._2 match {
        case IngestionReport(c, i, statusCode) =>
          c shouldBe "400"
          statusCode shouldBe 400
      }
    }

    it("completes the request with a 200") {
      val ctx = new ImperativeRequestContext {
        var completed: ToResponseMarshallable = _
        var error: Throwable = _

        override def complete(obj: ToResponseMarshallable): Unit =
          completed = obj

        override def failWith(error: Throwable): Unit = this.error = error
      }
      val req = HydraRequest("a44", "test payload")
      system.actorOf(HttpIngestionHandler.props(req, 3.seconds, ctx, registry))
      eventually {
        ctx.completed should not be null
      }
      ctx.completed.value.asInstanceOf[(_, _)]._2 match {
        case IngestionReport(c, _, statusCode) =>
          c shouldBe "a44"
          statusCode shouldBe 200
      }
    }

    it("fails the request on ingestion error") {
      val ctx = new ImperativeRequestContext {
        var completed: ToResponseMarshallable = _
        var error: Throwable = _

        override def complete(obj: ToResponseMarshallable): Unit =
          completed = obj

        override def failWith(error: Throwable): Unit = this.error = error
      }

      val req = HydraRequest("500", "error")
        .withMetadata(RequestParams.HYDRA_INGESTOR_PARAM -> "invalid_ingestor")
      system.actorOf(HttpIngestionHandler.props(req, 3.seconds, ctx, registry))
      eventually {
        ctx.error should not be null
      }
      ctx.error shouldBe an[ActorInitializationException]
    }
  }
}
