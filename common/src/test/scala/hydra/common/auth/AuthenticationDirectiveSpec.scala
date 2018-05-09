package hydra.common.auth

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.{BasicHttpCredentials, HttpCredentials}
import akka.http.scaladsl.server.{Directives, Route}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpecLike, Matchers}

import scala.concurrent.Future

class AuthenticationDirectiveSpec extends Matchers
  with FlatSpecLike
  with ScalaFutures
  with ScalatestRouteTest
  with AuthenticationDirectives
  with Directives {

  override val ec = scala.concurrent.ExecutionContext.Implicits.global

  def route = {
    val authenticator = new TestAuthenticator()
    val route = Route.seal {
      path("secured") {
        authenticateWith(authenticator) { user =>
          complete(user)
        }
      }
    }
    route
  }

  it should "allow auth when no creds are passed if configured that way" in {
    Get("/secured") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[String] shouldEqual "Anonymous"
    }
  }

  it should "return a 200" in {
    val validCredentials = BasicHttpCredentials("John", "p4ssw0rd")
    Get("/secured") ~> addCredentials(validCredentials) ~> route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[String] shouldEqual "John"
    }
  }

  it should "return a 401" in {
    val badCredentials = BasicHttpCredentials("unknown", "p4ssw0rd")
    Get("/secured") ~> addCredentials(badCredentials) ~> route ~> check {
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

  class TestAuthenticator extends HydraAuthenticator {
    override def auth(creds: Option[HttpCredentials]): Future[String] = {
      creds match {
        case Some(c) =>
          val c1 = c.asInstanceOf[BasicHttpCredentials]
          if (c1.username == "John") Future.successful("John") else Future.failed(new RuntimeException())
        case None => Future.successful("Anonymous")
      }
    }
  }
}
