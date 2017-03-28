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
