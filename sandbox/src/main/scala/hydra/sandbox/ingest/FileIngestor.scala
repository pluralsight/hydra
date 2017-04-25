package hydra.sandbox.ingest

import akka.actor.Props
import hydra.core.ingest.Ingestor
import hydra.core.protocol._
import hydra.sandbox.produce.{FileTransport, FileRecord}

/**
  * A simple example transport that writes all requests to a log, as configured by the application.
  *
  * Created by alexsilva on 2/27/17.
  */
class FileIngestor extends Ingestor {
  val fileProducer = context.actorOf(Props[FileTransport])

  ingest {
    case Publish(request) =>
      sender ! (if (request.metadataValue("hydra-file-stream").isDefined) Join else Ignore)

    case Ingest(request) =>
      fileProducer ! Produce(FileRecord(request.metadataValue("hydra-file-stream").get, request.payload))
      sender ! IngestorCompleted
  }
}
