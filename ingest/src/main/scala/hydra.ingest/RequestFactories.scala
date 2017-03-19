package hydra.ingest

import akka.http.scaladsl.model.HttpRequest
import akka.stream.Materializer
import hydra.core.ingest.{HydraRequest, RequestFactory}
import hydra.ingest.http.HttpRequestFactory

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by alexsilva on 3/14/17.
  */
object RequestFactories {

  implicit object RequestFactoryLikeHttp extends RequestFactory[String, HttpRequest] {
    override def createRequest(destination: Option[String], source: HttpRequest)
                              (implicit mat: Materializer): Future[HydraRequest] = {
      implicit val ec = mat.executionContext
      new HttpRequestFactory().createRequest(destination, source)
    }
  }

  def createRequest[P, D](destination: Option[String], source: D)
                         (implicit ev: RequestFactory[P, D], mat: Materializer): Future[HydraRequest] = {
    ev.createRequest(destination, source)
  }
}

