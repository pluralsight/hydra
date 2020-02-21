package hydra.core.ingest

import akka.stream.Materializer

import scala.concurrent.Future

/**
  * P - the payload type.
  * Created by alexsilva on 3/14/17.
  */
trait RequestFactory[S] {

  def createRequest(correlationId: String, source: S)(
      implicit mat: Materializer
  ): Future[HydraRequest]
}
