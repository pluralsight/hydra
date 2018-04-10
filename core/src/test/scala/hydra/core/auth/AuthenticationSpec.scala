package hydra.core.auth

import akka.http.scaladsl.model.headers.{BasicHttpCredentials, HttpCredentials, OAuth2BearerToken}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpecLike, Matchers}
import scala.concurrent.ExecutionContext.Implicits._

class AuthenticationSpec extends Matchers
  with FlatSpecLike
  with ScalaFutures {

  "The Authenticator" should "delegate auth" in {
    val authenticator = new TestHydraAuthenticator()
    whenReady(authenticator.authenticate(Some(new BasicHttpCredentials("test", "")))) { r =>
      r shouldBe Left(authenticator.challenge)
    }

    whenReady(authenticator.authenticate(Some(new BasicHttpCredentials("nice-user", "")))) { r =>
      r shouldBe Right("Basic")
    }

    whenReady(authenticator.authenticate(None)) { r =>
      r shouldBe Left(authenticator.challenge)
    }
  }
  "The NoSecurityAuthenticator" should "always return true" in {
    new NoSecurityAuthenticator().auth(new BasicHttpCredentials("test", "test")) shouldBe true
    new NoSecurityAuthenticator().auth(new OAuth2BearerToken("test")) shouldBe true
  }
  
  class TestHydraAuthenticator extends HydraAuthenticator {
    override def auth(creds: HttpCredentials): Boolean = {
      val c = creds.asInstanceOf[BasicHttpCredentials]
      c.username == "nice-user"
    }
  }

}
