package hydra.common.auth

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.{BasicHttpCredentials, HttpCredentials, OAuth2BearerToken}
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.{ExecutionContext, Future}

class AuthenticationSpec extends TestKit(ActorSystem("AuthenticationSpec"))
  with Matchers
  with FlatSpecLike
  with ScalaFutures
  with BeforeAndAfterAll {

  override def afterAll = TestKit.shutdownActorSystem(system)

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
    whenReady(new NoSecurityAuthenticator().auth(Some(new BasicHttpCredentials("test", "test")))) { f =>
      f shouldBe "Anonymous"
    }
    whenReady(new NoSecurityAuthenticator().auth(Some(new OAuth2BearerToken("test")))) { f =>
      f shouldBe "Anonymous"
    }
  }

  class TestAuthenticator extends HydraAuthenticator {
    override def auth(creds: Option[HttpCredentials])
                     (implicit s: ActorSystem, ec: ExecutionContext): Future[HydraPrincipal] = {
      val p = HydraPrincipal("nice-user",Set.empty)
      creds match {
        case Some(c) =>
          val c1 = c.asInstanceOf[BasicHttpCredentials]
          if (c1.username == "nice-user") Future.successful(p) else Future.failed(new RuntimeException())
        case None => Future.failed(new RuntimeException())
      }
    }
  }

}
