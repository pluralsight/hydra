package hydra.ingest.http

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.{MethodRejection, MissingHeaderRejection, RequestEntityExpectedRejection}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.testkit.{TestActorRef, TestKit}
import cats.effect.{Concurrent, ContextShift, IO}
import hydra.avro.registry.SchemaRegistry
import hydra.common.util.ActorUtils
import hydra.core.ingest.RequestParams
import hydra.core.marshallers.GenericError
import hydra.ingest.IngestorInfo
import hydra.ingest.services.IngestorRegistry.{FindAll, FindByName, LookupResult}
import hydra.ingest.test.TestIngestor
import hydra.kafka.algebras.KafkaClientAlgebra
import org.joda.time.DateTime
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
  * Created by alexsilva on 5/12/17.
  */
class IngestionEndpointSpec
    extends Matchers
    with AnyWordSpecLike
    with ScalatestRouteTest
    with HydraIngestJsonSupport {

  private implicit val timeout = RouteTestTimeout(10.seconds)
  val probe = system.actorOf(Props[TestIngestor])

  val ingestorInfo =
    IngestorInfo(ActorUtils.actorName(probe), "test", probe.path, DateTime.now)

  val registry = TestActorRef(
    new Actor {

      override def receive = {
        case FindByName(name) if name == "tester" =>
          sender ! LookupResult(Seq(ingestorInfo))
        case FindByName(name) if name == "error" =>
          throw new IllegalArgumentException("RAR")
        case FindByName(_) => sender ! LookupResult(Seq.empty)
        case FindAll       => sender ! LookupResult(Seq(ingestorInfo))
      }
    },
    "ingestor_registry"
  ).underlyingActor

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val concurrentEffect: Concurrent[IO] = IO.ioConcurrentEffect

  import scalacache.Mode
  implicit val mode: Mode[IO] = scalacache.CatsEffect.modes.async
  val ingestRoute = new IngestionEndpoint(
    false,
    new IngestionFlow[IO](SchemaRegistry.test[IO].unsafeRunSync, KafkaClientAlgebra.test[IO].unsafeRunSync)
  ).route

  override def afterAll = {
    super.afterAll()
    TestKit.shutdownActorSystem(
      system,
      verifySystemShutdown = true,
      duration = 10 seconds
    )
  }

  "The ingestor endpoint" should {

    "rejects a GET request" in {
      Get("/ingest") ~> ingestRoute ~> check {
        rejections should contain allElementsOf (Seq(
          MethodRejection(HttpMethods.POST),
          MethodRejection(HttpMethods.DELETE)
        ))
      }
    }

    "rejects empty requests" in {
      Post("/ingest") ~> ingestRoute ~> check {
        rejection shouldEqual RequestEntityExpectedRejection
      }
    }

    "initiates a delete request" in {
      val key = RawHeader(RequestParams.HYDRA_RECORD_KEY_PARAM, "test")
      Delete("/ingest").withHeaders(key) ~> ingestRoute ~> check {
        response.status.intValue() shouldBe 200 //todo: should we support a 204?
      }
    }

    "rejects a delete request without a key" in {
      Delete("/ingest") ~> ingestRoute ~> check {
        rejection shouldEqual MissingHeaderRejection("hydra-record-key")
      }
    }

    "publishes to a target ingestor" in {
      val ingestor = RawHeader(RequestParams.HYDRA_INGESTOR_PARAM, "tester")
      val request = Post("/ingest", "payload").withHeaders(ingestor)
      request ~> ingestRoute ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    "rejects a request with an invalid ack strategy" in {
      val ingestor = RawHeader(RequestParams.HYDRA_INGESTOR_PARAM, "tester")
      val request = Post("/ingest", "payload").withHeaders(
        ingestor,
        RawHeader(RequestParams.HYDRA_ACK_STRATEGY, "invalid")
      )
      request ~> ingestRoute ~> check {
        status shouldBe StatusCodes.BadRequest
        entityAs[GenericError].status shouldBe 400
      }
    }

    "returns 404 if unknown ingestor" in {
      val ingestor = RawHeader(RequestParams.HYDRA_INGESTOR_PARAM, "unknown")
      val request = Post("/ingest", "payload").withHeaders(ingestor)
      request ~> ingestRoute ~> check {
        status shouldBe StatusCodes.NotFound
      }
    }

    "broadcasts a request" in {
      val request = Post("/ingest", "payload")
      request ~> ingestRoute ~> check {
        status shouldBe StatusCodes.OK
      }
    }
  }
}
