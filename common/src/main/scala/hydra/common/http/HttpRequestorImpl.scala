package hydra.common.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}

import scala.concurrent.Future

class HttpRequestorImpl(implicit sys: ActorSystem) extends HttpRequestor {
  override def makeRequest(request: HttpRequest): Future[HttpResponse] =
    Http().singleRequest(request)
}
