package hydra.core.auth

import akka.http.scaladsl.model.headers.{BasicHttpCredentials, HttpCredentials, OAuth2BearerToken}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpecLike, Matchers}

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future

class AuthenticationSpec extends Matchers
  with FlatSpecLike
  with ScalaFutures {

  "The Authenticator" should "delegate auth" in {
    val authenticator = new TestAuthenticator()

    whenReady(authenticator.authenticate(Some(new BasicHttpCredentials("test", "")))) { r =>
      r shouldBe Left(authenticator.challenge)
    }

    whenReady(authenticator.authenticate(Some(new BasicHttpCredentials("nice-user", "")))) { r =>
      r shouldBe Right("nice-user")
    }

    whenReady(authenticator.authenticate(None)) { r =>
      r shouldBe Left(authenticator.challenge)
    }
  }
  "The NoSecurityAuthenticator" should "always return true" in {
    new NoSecurityAuthenticator().auth(Some(new BasicHttpCredentials("test", "test"))) shouldBe Future.successful("Anonymous")
    new NoSecurityAuthenticator().auth(Some(new OAuth2BearerToken("test"))) shouldBe Future.successful("Anonymous")
  }

  class TestAuthenticator extends HydraAuthenticator {
    override def auth(creds: Option[HttpCredentials]): Future[String] = {
      creds match {
        case Some(c) =>
          val c1 = c.asInstanceOf[BasicHttpCredentials]
          if (c1.username == "nice-user") Future.successful("nice-user") else Future.failed(new RuntimeException())
        case None => Future.failed(new RuntimeException())
      }
    }
  }
}
