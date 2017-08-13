package hydra.ingest.protocol

import hydra.core.ingest.HydraRequestMetadata
import hydra.core.protocol.{HydraMessage, IngestorStatus}

/**
  * Created by alexsilva on 2/22/17.
  */
case class IngestionReport(correlationId: Long,
                           metadata: Seq[HydraRequestMetadata],
                           ingestors: Map[String, IngestorStatus],
                           statusCode: Int) extends HydraMessage
