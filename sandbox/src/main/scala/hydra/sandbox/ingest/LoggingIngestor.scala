package hydra.sandbox.ingest

import hydra.core.ingest.Ingestor
import hydra.core.protocol._

/**
  * A simple example transport that writes requests with a certain attribute to a log.
  *
  * Created by alexsilva on 2/27/17.
  */
class LoggingIngestor extends Ingestor {
  ingest {
    case Publish(request) =>
      sender ! (if (request.metadataValueEquals("logging-enabled", "true")) Join else Ignore)

    case Ingest(request) =>
      log.info(request.payload.toString)
      sender ! IngestorCompleted
  }
}
