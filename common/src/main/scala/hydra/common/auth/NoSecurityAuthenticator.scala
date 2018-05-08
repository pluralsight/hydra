package hydra.common.auth

import akka.http.scaladsl.model.headers.HttpCredentials

import scala.concurrent.Future

class NoSecurityAuthenticator extends HydraAuthenticator {
  override def auth(creds: Option[HttpCredentials]): Future[String] = {
    Future.successful("Anonymous")
  }
}
