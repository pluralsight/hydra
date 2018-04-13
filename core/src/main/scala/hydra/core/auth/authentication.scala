package hydra.core.auth

import akka.http.scaladsl.model.headers.{HttpChallenge, HttpCredentials}
import akka.http.scaladsl.server.directives.{AuthenticationDirective, SecurityDirectives}
import hydra.core.Settings

import scala.concurrent.{ExecutionContext, Future}

trait AuthenticationDirectives extends SecurityDirectives {

  private val authenticator = Settings.HydraSettings.Authenticator

  protected[this] implicit def ec: ExecutionContext

  def authenticate: AuthenticationDirective[String] =
    authenticateOrRejectWithChallenge(authenticator.authenticate _)

  def authenticateWith(authenticator: HydraAuthenticator): AuthenticationDirective[String] =
    authenticateOrRejectWithChallenge(authenticator.authenticate _)
}

trait HydraAuthenticator {

  type AuthenticationResult[+T] = Either[HttpChallenge, T]

  val challenge = HttpChallenge("Hydra", Some("Hydra"))

  def auth(credentials: Option[HttpCredentials]): Future[String]

  def authenticate(credentials: Option[HttpCredentials])
                  (implicit ec: ExecutionContext): Future[AuthenticationResult[String]] = {
    auth(credentials)
      .map(Right(_))
      .recover {
        case _: Throwable => Left(challenge)
      }
  }
}

class NoSecurityAuthenticator extends HydraAuthenticator {
  override def auth(creds: Option[HttpCredentials]): Future[String] = {
    Future.successful("Anonymous")
  }
}