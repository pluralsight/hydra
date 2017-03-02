package hydra.ingest.protocol

import hydra.core.protocol.HydraMessage

/**
  * Created by alexsilva on 2/22/17.
  */
case class IngestionCompleted(s: IngestionStatus) extends HydraMessage

