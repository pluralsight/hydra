package hydra.sandbox.ingest

import hydra.core.ingest.{HydraRequest, Ingestor, TransportOps}
import hydra.core.protocol._
import hydra.core.transport.RecordFactory
import hydra.sandbox.transport.FileRecord

import scala.util.Success

/**
  * A simple example transport that writes all requests to a log, as configured by the application.
  *
  * Created by alexsilva on 2/27/17.
  */
class FileIngestor extends Ingestor with TransportOps {
  override def transportName = "file"

  ingest {
    case Publish(request) =>
      sender ! (if (request.metadataValue("hydra-file-stream").isDefined) Join else Ignore)

    case Ingest(r, supervisor, ack) => transport(r, supervisor, ack)
  }

  override val recordFactory = FileRecordFactory
}

object FileRecordFactory extends RecordFactory[String, String] {
  override def build(r: HydraRequest) = Success(FileRecord(r.metadataValue("hydra-file-stream").get, r.payload))
}

