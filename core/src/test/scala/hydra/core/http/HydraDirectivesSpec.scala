package hydra.core.http

import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike

class HydraDirectivesSpec
    extends Matchers
    with AnyFunSpecLike
    with ScalatestRouteTest
    with HydraDirectives {

  describe("Hydra Directives") {
    it("completes with location header") {
      Get() ~> completeWithLocationHeader(StatusCodes.OK, 123) ~> check {
        header[Location].get.uri shouldBe Uri("http://example.com/123")
      }
    }
    it("imperatively completes") {
      Get() ~> imperativelyComplete((ctx) => ctx.complete("DONE")) ~> check {
        responseAs[String] shouldBe "DONE"
      }
    }
  }

}
