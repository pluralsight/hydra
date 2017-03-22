package hydra.core.examples.ingest

import hydra.core.ingest.Ingestor
import hydra.core.protocol._

/**
  * A simple example transport that writes requests to the log, as configured by the application.
  *
  * Created by alexsilva on 2/27/17.
  */
class LoggingIngestor extends Ingestor {
  ingest {
    case Publish(request) =>
      sender ! Join

    case Ingest(request) =>
      log.info(request.payload.toString)
      sender ! IngestorCompleted
  }
}
