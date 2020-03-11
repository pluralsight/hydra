package hydra.ingest.http

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class HealthEndpointSpec
    extends Matchers
    with AnyFlatSpecLike
    with ScalatestRouteTest {

  "The HealthEndpoint" should "return a 200" in {
    Get("/health") ~> HealthEndpoint.route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }
}
