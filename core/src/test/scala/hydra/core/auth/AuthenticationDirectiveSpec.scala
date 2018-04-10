package hydra.core.auth

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.{BasicHttpCredentials, HttpChallenge, HttpCredentials}
import akka.http.scaladsl.server.{Directives, Route}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpecLike, Matchers}
import akka.http.scaladsl.model.headers._

class AuthenticationDirectiveSpec extends Matchers
  with FlatSpecLike
  with ScalaFutures
  with ScalatestRouteTest
  with AuthenticationDirectives
  with Directives {

  override val ec = scala.concurrent.ExecutionContext.Implicits.global

  def route(allowIfNoCreds: Boolean) = {
    val authenticator = new TestAuthenticator(allowIfNoCreds)
    val route = Route.seal {
      path("secured") {
        authenticateWith(authenticator) { user =>
          complete(user)
        }
      }
    }
    route
  }

  "The directive" should "deny auth when no creds are passed" in {
    Get("/secured") ~> route(false) ~> check {
      status shouldEqual StatusCodes.Unauthorized
      responseAs[String] shouldEqual
        "The resource requires authentication, which was not supplied with the request"
      header[`WWW-Authenticate`].get.challenges.head shouldEqual HttpChallenge("Hydra", Some("Hydra"))
    }
  }

  it should "allow auth when no creds are passed if configured that way" in {
    Get("/secured") ~> route(true) ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[String] shouldEqual "Anonymous"
    }
  }

  it should "return a 200" in {
    val validCredentials = BasicHttpCredentials("John", "p4ssw0rd")
    Get("/secured") ~> addCredentials(validCredentials) ~> route(false) ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[String] shouldEqual "Basic"
    }
  }

  it should "return a 401" in {
    val badCredentials = BasicHttpCredentials("unknown", "p4ssw0rd")
    Get("/secured") ~> addCredentials(badCredentials) ~> route(false) ~> check {
      status shouldEqual StatusCodes.Unauthorized
    }
  }

  it should "use the configured authenticator" in {
    val route = Route.seal {
      path("secured") {
        authenticate { user =>
          complete(user)
        }
      }
    }

    Get("/secured") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[String] shouldEqual "Anonymous"
    }
  }

  class TestAuthenticator(override val allowIfNoCreds: Boolean) extends HydraAuthenticator {
    override def auth(creds: HttpCredentials): Boolean = {
      val c = creds.asInstanceOf[BasicHttpCredentials]
      c.username == "John"
    }
  }

}
