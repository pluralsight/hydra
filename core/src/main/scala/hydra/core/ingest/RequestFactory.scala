package hydra.core.ingest

import akka.stream.Materializer

import scala.concurrent.{ExecutionContext, Future}

/**
  * P - the payload type.
  * Created by alexsilva on 3/14/17.
  */
trait RequestFactory[P, S] {
  def createRequest(label: Option[String], source: S)(implicit mat:Materializer): Future[HydraRequest]
}
