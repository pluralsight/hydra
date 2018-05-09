package hydra.common.auth

import akka.http.scaladsl.model.headers.HttpCredentials

import scala.concurrent.{ExecutionContext, Future}

class NoSecurityAuthenticator extends HydraAuthenticator {
  override def auth(creds: Option[HttpCredentials])
                   (implicit ec: ExecutionContext): Future[String] = {
    Future.successful("Anonymous")
  }
}
