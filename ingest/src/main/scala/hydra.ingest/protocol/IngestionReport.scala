package hydra.ingest.protocol

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import hydra.core.ingest.{HydraRequest, HydraRequestMetadata}
import hydra.core.protocol.{HydraMessage, IngestorCompleted, IngestorStatus}

/**
  * Created by alexsilva on 2/22/17.
  */
case class IngestionReport(correlationId: String,
                           metadata: Seq[HydraRequestMetadata],
                           ingestors: Map[String, IngestorStatus],
                           statusCode: StatusCode) extends HydraMessage


object IngestionReport {
  def apply(request: HydraRequest, ingestors: Map[String, IngestorStatus]): IngestionReport = {
    val statusCode = {
      if (ingestors.isEmpty) {
        StatusCodes.custom(404, "No ingestor joined the request.")
      }
      else {
        ingestors.filter(_._2 != IngestorCompleted).values.headOption map (_.statusCode) getOrElse StatusCodes.OK
      }
    }

    IngestionReport(request.correlationId, request.metadata, ingestors, statusCode)
  }
}