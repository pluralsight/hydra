package hydra.common.auth

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.{HttpChallenge, HttpCredentials}

import scala.concurrent.{ExecutionContext, Future}

trait HydraAuthenticator {

  type AuthenticationResult[+T] = Either[HttpChallenge, T]

  val challenge = HttpChallenge("Hydra", Some("Hydra"))

  def auth(credentials: Option[HttpCredentials])
          (implicit system: ActorSystem, ec: ExecutionContext): Future[HydraPrincipal]

  def authenticate(credentials: Option[HttpCredentials])
                  (implicit system: ActorSystem, ec: ExecutionContext): Future[AuthenticationResult[HydraPrincipal]] = {
    auth(credentials)
      .map(Right(_))
      .recover {
        case _: Throwable => Left(challenge)
      }
  }
}
