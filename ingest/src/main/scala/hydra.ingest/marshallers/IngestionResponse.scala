package hydra.ingest.marshallers

import akka.http.scaladsl.model.StatusCodes._
import hydra.ingest.protocol.IngestionStatus

/**
  * Created by alexsilva on 10/31/16.
  */

case class IngestionResponse(status: Int, result: IngestionStatus)

object IngestionResponse {
  def apply(h: IngestionStatus): IngestionResponse = IngestionResponse(h.ingestionStatus.intValue(), h)
}

case class IngestionErrorResponse(status: Int, message: String)

object IngestionErrorResponse {
  def serverError(msg: String): IngestionErrorResponse =
    IngestionErrorResponse(InternalServerError.intValue, msg)

  def requestError(msg: String): IngestionErrorResponse =
    IngestionErrorResponse(BadRequest.intValue, msg)

  def ingestionTimedOut(msg: String): IngestionErrorResponse =
    IngestionErrorResponse(RequestTimeout.intValue, msg)
}