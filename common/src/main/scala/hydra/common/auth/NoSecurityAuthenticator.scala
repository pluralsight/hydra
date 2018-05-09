package hydra.common.auth

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.HttpCredentials

import scala.concurrent.{ExecutionContext, Future}

class NoSecurityAuthenticator extends HydraAuthenticator {
  override def auth(creds: Option[HttpCredentials])
                   (implicit system: ActorSystem, ec: ExecutionContext): Future[String] = {
    Future.successful("Anonymous")
  }
}
