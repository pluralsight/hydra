package hydra.ingest.endpoints

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpMethods, StatusCodes}
import akka.http.scaladsl.server.{MethodRejection, RequestEntityExpectedRejection}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.TestKit
import org.scalatest.{Matchers, WordSpecLike}

/**
  * Created by alexsilva on 5/12/17.
  */
class IngestEndpointSpec extends TestKit(ActorSystem("hydra_test")) with WordSpecLike with Matchers
  with ScalatestRouteTest {

  val ingestRoute = new IngestionEndpoint().route

  "The ingestor endpoint" should {

    "rejects a GET request" in {
      Get() ~> ingestRoute ~> check {
        rejection shouldEqual MethodRejection(HttpMethods.POST)
      }
    }

    "rejects empty requests" in {
      Post() ~> ingestRoute ~> check {
        rejection shouldEqual RequestEntityExpectedRejection
      }
    }

    "publishes to a target ingestor" in {
      val request = Post("/", "payload")
        .addHeader(RawHeader("hydra-ingestor", "tester"))
      request ~> ingestRoute ~> check {
        status shouldBe StatusCodes.OK
      }
    }
  }
}
