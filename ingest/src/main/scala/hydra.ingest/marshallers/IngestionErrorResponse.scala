package hydra.ingest.marshallers

import akka.http.scaladsl.model.StatusCodes._

/**
  * Created by alexsilva on 10/31/16.
  */
case class IngestionErrorResponses(status: Int, message: String)

object IngestionErrorResponses {
  def serverError(msg: String): IngestionErrorResponses =
    IngestionErrorResponses(InternalServerError.intValue, msg)

  def requestError(msg: String): IngestionErrorResponses =
    IngestionErrorResponses(BadRequest.intValue, msg)

  def ingestionTimedOut(msg: String): IngestionErrorResponses =
    IngestionErrorResponses(RequestTimeout.intValue, msg)
}