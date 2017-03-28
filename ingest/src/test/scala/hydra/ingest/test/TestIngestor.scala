package hydra.ingest.test

import hydra.core.ingest.Ingestor
import hydra.core.protocol.{Ingest, IngestorCompleted, Join, Publish}

/**
  * Created by alexsilva on 3/26/17.
  */
class TestIngestor extends Ingestor {
  ingest {
    case Publish(_) =>
      sender ! Join

    case Ingest(request) =>
      log.info(request.payload.toString)
      sender ! IngestorCompleted
  }
}
