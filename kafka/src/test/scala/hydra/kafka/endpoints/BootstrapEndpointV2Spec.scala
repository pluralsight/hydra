package hydra.kafka.endpoints

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.{Matchers, WordSpec}

class BootstrapEndpointV2Spec extends WordSpec with ScalatestRouteTest with Matchers {

  val bootstrapRoute: Route = new BootstrapEndpointV2().route

  "BootstrapEndpointV2" must {

    "accept a valid request payload with a 200 OK" in {
      Put("/streams") ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    "reject on paths beyond `/streams`" in {
      Put("/streams/foo") ~> Route.seal(bootstrapRoute) ~> check {
        status shouldBe StatusCodes.NotFound
      }
    }

  }
}
