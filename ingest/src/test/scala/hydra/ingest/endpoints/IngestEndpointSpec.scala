package hydra.ingest.endpoints

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.{HttpMethods, StatusCodes}
import akka.http.scaladsl.server.{MethodRejection, RequestEntityExpectedRejection}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.TestActorRef
import hydra.common.util.ActorUtils
import hydra.ingest.ingestors.IngestorInfo
import hydra.ingest.services.IngestorRegistry.{FindByName, LookupResult}
import hydra.ingest.test.TestIngestor
import org.joda.time.DateTime
import org.scalatest.{Matchers, WordSpecLike}

/**
  * Created by alexsilva on 5/12/17.
  */
class IngestEndpointSpec extends Matchers with WordSpecLike with ScalatestRouteTest {

  val probe = system.actorOf(Props[TestIngestor])
  val ingestorInfo = IngestorInfo(ActorUtils.actorName(probe), "test", probe.path, DateTime.now)
  val registry = TestActorRef(new Actor {
    override def receive = {
      case FindByName("tester") => sender ! LookupResult(Seq(ingestorInfo))
    }
  }, "ingestor_registry").underlyingActor

  val ingestRoute = new IngestionEndpoint().route

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
  }
}
