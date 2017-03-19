package hydra.core.ingest

import akka.stream.Materializer

import scala.concurrent.{ExecutionContext, Future}

/**
  * PT - the payload type.  Even though it is not used in the method,
  * we need to pass it so that we know what we should unmarshall the
  * request to
  * Created by alexsilva on 3/14/17.
  */
trait RequestFactory[P, S] {
  def createRequest(label: Option[String], source: S)(implicit mat:Materializer): Future[HydraRequest]
}
