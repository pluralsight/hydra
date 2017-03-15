package hydra.ingest.http

import akka.http.scaladsl.server.RequestContext
import hydra.core.ingest.{HydraRequest, HydraRequestMedatata, RequestFactory}

/**
  * Created by alexsilva on 3/14/17.
  */
class HttpRequestFactory extends RequestFactory[String, RequestContext] {
  //todo: add retry strategy, etc. stuff
  override def createRequest(destination: String, payload: String, ctx: RequestContext): HydraRequest = {
    val metadata: List[HydraRequestMedatata] = List(ctx.request.headers.map(header =>
      HydraRequestMedatata(header.name.toLowerCase, header.value)): _*)
    HydraRequest(destination, payload, metadata)
  }
}
