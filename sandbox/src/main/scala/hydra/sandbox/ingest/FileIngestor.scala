package hydra.sandbox.ingest

import akka.actor.Props
import hydra.core.ingest.{HydraRequest, Ingestor}
import hydra.core.protocol._
import hydra.core.transport.RecordFactory
import hydra.sandbox.transport.{FileRecord, FileTransport}

import scala.util.Success

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

    case Ingest(r) =>
      fileProducer ! Produce(r, self, sender)
      sender ! IngestorCompleted
  }

  override val recordFactory = new RecordFactory[String, String] {
    override def build(r: HydraRequest) = Success(FileRecord(r.metadataValue("hydra-file-stream").get, r.payload))
  }
}


