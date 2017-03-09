package hydra.ingest.services

import hydra.core.ingest.Ingestor
import hydra.core.protocol._

/**
  * Created by alexsilva on 3/9/17.
  */
class TestIngestor extends Ingestor {
  ingest {
    case Publish(request) => sender ! Join

    case Validate(request) => sender ! ValidRequest

    case Ingest(request) => sender ! IngestorCompleted
  }
}
