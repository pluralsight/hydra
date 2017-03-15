package hydra.ingest

import akka.http.scaladsl.server.RequestContext
import hydra.core.ingest.{HydraRequest, RequestFactory}
import hydra.ingest.http.HttpRequestFactory

/**
  * Created by alexsilva on 3/14/17.
  */
object RequestFactories {

  implicit object RequestFactoryLikeHttp extends RequestFactory[String, RequestContext] {
    override def createRequest(destination: String, payload: String, data: RequestContext): HydraRequest = {
      new HttpRequestFactory().createRequest(destination, payload, data)
    }
  }

  def createRequest[P, D](destination: String, payload: P, data: D)(implicit ev: RequestFactory[P, D]): HydraRequest = {
    ev.createRequest(destination, payload, data)
  }
}

