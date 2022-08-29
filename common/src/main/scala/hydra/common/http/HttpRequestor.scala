package hydra.common.http

import akka.http.scaladsl.model.{HttpRequest, HttpResponse}

import scala.concurrent.Future

trait HttpRequestor {
  def makeRequest(request: HttpRequest): Future[HttpResponse]
}