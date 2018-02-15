package hydra.ingest.http

import akka.http.scaladsl.model.{HttpMethods, HttpRequest}
import akka.http.scaladsl.server.directives.CodingDirectives
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import hydra.core.ingest.RequestParams._
import hydra.core.ingest._
import hydra.core.transport.{AckStrategy, ValidationStrategy}

import scala.concurrent.Future

/**
  * Created by alexsilva on 3/14/17.
  */
class HttpRequestFactory extends RequestFactory[HttpRequest] with CodingDirectives {

  override def createRequest(correlationId: String, request: HttpRequest)
                            (implicit mat: Materializer): Future[HydraRequest] = {
    implicit val ec = mat.executionContext

    lazy val vs = request.headers.find(_.lowercaseName() == HYDRA_VALIDATION_STRATEGY)
      .map(h => ValidationStrategy(h.value())).getOrElse(ValidationStrategy.Strict)

    lazy val as = request.headers.find(_.lowercaseName() == HYDRA_ACK_STRATEGY)
      .map(h => AckStrategy(h.value())).getOrElse(AckStrategy.NoAck)

    Unmarshal(request.entity).to[String].map { payload =>
      val dPayload = if (request.method == HttpMethods.DELETE && payload.isEmpty) null else payload
      val metadata: Map[String, String] = request.headers.map(h => h.name.toLowerCase -> h.value).toMap
      HydraRequest(correlationId, dPayload, metadata, validationStrategy = vs, ackStrategy = as)
    }
  }
}

