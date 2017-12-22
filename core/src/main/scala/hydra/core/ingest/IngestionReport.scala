package hydra.core.ingest

import hydra.core.protocol.{HydraMessage, IngestorStatus}

/**
  * Created by alexsilva on 2/22/17.
  */
case class IngestionReport(correlationId: Long,
                           ingestors: Map[String, IngestorStatus],
                           statusCode: Int,
                           replyTo: Option[String] = None) extends HydraMessage
