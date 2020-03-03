package hydra.core.ingest

import hydra.core.protocol.{HydraMessage, IngestorStatus}

/**
  * Created by alexsilva on 2/22/17.
  */
case class IngestionReport(
    correlationId: String,
    ingestors: Map[String, IngestorStatus],
    statusCode: Int
) extends HydraMessage
