package hydra.core.auth

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, GenericHttpCredentials, HttpCredentials}
import akka.stream.ActorMaterializer
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class HydraAuthenticationClient(implicit sys: ActorSystem, ec: ExecutionContext) extends HydraAuthenticator
  with LoggingAdapter {

  implicit val mat = ActorMaterializer()

  override def auth(credentials: Option[HttpCredentials]): Future[String] = {
    import HydraAuthenticationClient._

    val token = credentials match {
      case Some(creds) => creds.token
      case None => return Future.failed(MissingCredentialsException())
    }

    Http()
      .singleRequest(buildRequest(token))
      .flatMap(extractUserName)
  }
}


object HydraAuthenticationClient extends ConfigSupport with HydraAuthenticationFormat {
  def buildRequest(token: String): HttpRequest = {
    val serverUrl = applicationConfig.getString("auth.serverUrl")

    val serverPort = applicationConfig.getInt("auth.serverPort")

    val authUri = Uri(serverUrl).withPort(serverPort).withPath(Uri.Path("hydra-auth/whoami"))

    val auth = Authorization(GenericHttpCredentials("Bearer", token))

    HttpRequest(HttpMethods.POST, authUri, List(auth))
  }

  def extractUserName(response: HttpResponse)
                     (implicit mat: ActorMaterializer, ec: ExecutionContext): Future[String] = {
    response match {
      case HttpResponse(StatusCodes.OK, _, entity, _) =>
        entity.toStrict(500.milliseconds).map(_.data.utf8String).map { jsonString =>
          jsonString.parseJson.convertTo[User].username
        }

      case HttpResponse(failureCode, _, entity, _) =>
        entity.toStrict(500.milliseconds).map(_.data.utf8String).map { errorMsg =>
          throw AuthenticationFailureException(errorMsg)
        }
    }
  }

  case class AuthenticationFailureException(msg: String) extends Exception(msg)

  case class MissingCredentialsException(msg: String = "Credentials are required for authentication.") extends Exception(msg)
}
