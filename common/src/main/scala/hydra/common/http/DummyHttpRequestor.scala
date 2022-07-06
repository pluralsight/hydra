package hydra.common.http

import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import scala.concurrent.Future

class DummyHttpRequestor extends HttpRequestor {
  override def makeRequest(request: HttpRequest): Future[HttpResponse] = {
    throw new NotImplementedException()
  }
}
