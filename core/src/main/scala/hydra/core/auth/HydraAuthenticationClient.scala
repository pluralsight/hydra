package hydra.core.auth

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.{Authorization, GenericHttpCredentials, HttpCredentials}
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse, Uri}
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

class HydraAuthenticationClient(implicit sys: ActorSystem, ec: ExecutionContext) extends HydraAuthenticator
  with LoggingAdapter {
  override def auth(credentials: Option[HttpCredentials]): Future[String] = {
    import HydraAuthenticationClient._

    val token = credentials match {
      case Some(creds) => creds.token
      case None => ""
    }

    val response: Future[HttpResponse] = Http().singleRequest(buildRequest(token))

    var result: Option[String] = None

    response.onComplete {
      case Success(resp) => result = Some(parseResponse(resp))
      case _ => result = None
    }

    result
  }
}


object HydraAuthenticationClient extends ConfigSupport {
  def buildRequest(token: String): HttpRequest = {
    val serverUrl = applicationConfig.getString("auth.serverUrl")

    val serverPort = applicationConfig.getInt("auth.serverPort")

    val authUri = Uri(serverUrl).withPort(serverPort).withPath(Uri.Path("hydra-auth/me"))

    val auth = Authorization(GenericHttpCredentials("Bearer", token))

    HttpRequest(HttpMethods.POST, authUri, List(auth))
  }

  def parseResponse(response: HttpResponse): String = {
    ""
  }
}
