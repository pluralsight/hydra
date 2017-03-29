package hydra.examples.ingest

import akka.actor.Props
import hydra.core.ingest.Ingestor
import hydra.core.protocol._
import hydra.examples.produce.{FileProducer, FileRecord}

/**
  * A simple example transport that writes all requests to a log, as configured by the application.
  *
  * Created by alexsilva on 2/27/17.
  */
class FileIngestor extends Ingestor {
  val fileProducer = context.actorOf(Props[FileProducer])

  ingest {
    case Publish(request) =>
      sender ! (if (request.metadataValue("file-stream").isDefined) Join else Ignore)

    case Ingest(request) =>
      fileProducer ! Produce(FileRecord(request.metadataValue("file-stream").get, request.payload))
      sender ! IngestorCompleted
  }
}
