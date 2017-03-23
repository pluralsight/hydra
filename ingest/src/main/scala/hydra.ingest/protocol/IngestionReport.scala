package hydra.ingest.protocol

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import hydra.core.ingest.{HydraRequestMedatata, IngestorState}
import hydra.core.protocol.{HydraMessage, IngestorCompleted}

/**
  * Created by alexsilva on 2/22/17.
  */
case class IngestionReport(correlationId: String,
                           metadata: Seq[HydraRequestMedatata],
                           ingestors: Map[String, IngestorState],
                           statusCode: StatusCode) extends HydraMessage

object IngestionReport {
  def apply(status: IngestionStatus): IngestionReport = {
    val errors = status.ingestors.filter(_._2.status != IngestorCompleted).values.toSeq
    val statusCode = if (!errors.isEmpty) errors.head.status.statusCode else StatusCodes.OK

    IngestionReport(status.request.correlationId, status.request.metadata, status.ingestors, statusCode)
  }
}

