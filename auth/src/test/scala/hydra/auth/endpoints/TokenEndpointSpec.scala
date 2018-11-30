package hydra.auth.endpoints

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.MethodRejection
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import hydra.common.config.ConfigSupport
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._


class TokenEndpointSpec extends Matchers
  with WordSpecLike
  with ScalatestRouteTest
  with ConfigSupport {

  private implicit val timeout = RouteTestTimeout(10.seconds)



  private val tokenRoute = new TokenEndpoint().route


  override def beforeAll: Unit = {
    
  }

  override def afterAll = {
    super.afterAll()
  }

  "The token endpoint" should {
    "complete a GET request" in {
      Get("/token") ~> tokenRoute ~> check {
        rejections should contain allElementsOf Seq(MethodRejection(HttpMethods.POST))
      }
    }

  }
}