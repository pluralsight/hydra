package hydra.ingest.bootstrap

import akka.http.scaladsl.model.HttpRequest
import akka.stream.Materializer
import hydra.core.ingest.{HydraRequest, RequestFactory}
import hydra.ingest.http.HttpRequestFactory

import scala.concurrent.Future

/**
  * Created by alexsilva on 3/14/17.
  */
object RequestFactories {

  implicit object RequestFactoryLikeHttp extends RequestFactory[String, HttpRequest] {
    override def createRequest(correlationId:Long, source: HttpRequest)
                              (implicit mat: Materializer): Future[HydraRequest] = {
      implicit val ec = mat.executionContext
      new HttpRequestFactory().createRequest(correlationId, source)
    }
  }

  def createRequest[P, D](correlationId: Long, source: D)
                         (implicit ev: RequestFactory[P, D], mat: Materializer): Future[HydraRequest] = {
    ev.createRequest(correlationId, source)
  }
}

