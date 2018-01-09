package hydra.ingest.http

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.{HttpMethods, StatusCodes}
import akka.http.scaladsl.server.{MethodRejection, RequestEntityExpectedRejection}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.testkit.{TestActorRef, TestKit}
import hydra.common.util.ActorUtils
import hydra.ingest.IngestorInfo
import hydra.ingest.services.IngestorRegistry.{FindAll, FindByName, LookupResult}
import hydra.ingest.test.TestIngestor
import org.joda.time.DateTime
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._

/**
  * Created by alexsilva on 5/12/17.
  */
class IngestEndpointSpec extends Matchers with WordSpecLike with ScalatestRouteTest {

  private implicit val timeout = RouteTestTimeout(10.seconds)
  val probe = system.actorOf(Props[TestIngestor])
  val ingestorInfo = IngestorInfo(ActorUtils.actorName(probe), "test", probe.path, DateTime.now)
  val registry = TestActorRef(new Actor {
    override def receive = {
      case FindByName(name) if name == "tester" => sender ! LookupResult(Seq(ingestorInfo))
      case FindByName(name) if name == "error" => throw new IllegalArgumentException("RAR")
      case FindByName(_) => sender ! LookupResult(Seq.empty)
      case FindAll => sender ! LookupResult(Seq(ingestorInfo))
    }
  }, "ingestor_registry").underlyingActor

  val ingestRoute = new IngestionEndpoint().route

  override def afterAll = {
    super.afterAll()
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true, duration = 10 seconds)
  }

  "The ingestor endpoint" should {

    "rejects a GET request" in {
      Get("/ingest") ~> ingestRoute ~> check {
        rejection shouldEqual MethodRejection(HttpMethods.POST)
      }
    }

    "rejects empty requests" in {
      Post("/ingest") ~> ingestRoute ~> check {
        rejection shouldEqual RequestEntityExpectedRejection
      }
    }

    "publishes to a target ingestor" in {
      val request = Post("/ingest/tester", "payload")
      request ~> ingestRoute ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    "returns 404 if unknown ingestor" in {
      val request = Post("/ingest/unknown", "payload")
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
