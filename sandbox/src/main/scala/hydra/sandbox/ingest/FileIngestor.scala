package hydra.sandbox.ingest

import com.typesafe.config.{Config, ConfigFactory}
import hydra.common.config.ConfigSupport
import hydra.core.ingest.{HydraRequest, Ingestor, TransportOps}
import hydra.core.protocol._
import hydra.core.transport.{HydraRecord, RecordFactory}
import hydra.sandbox.transport.FileRecord

import scala.util.{Failure, Success, Try}

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

    case Ingest(r, ack) => transport(r, ack)

  }

  override val recordFactory = FileRecordFactory
}

object FileRecordFactory extends RecordFactory[String, String] with ConfigSupport {

  import configs.syntax._


  private val destinations = ConfigSupport.toMap(applicationConfig
    .getOrElse[Config]("transports.file.destinations", ConfigFactory.empty).value)

  override def build(r: HydraRequest): Try[HydraRecord[String, String]] = {
    val file = r.metadataValue("hydra-file-stream").get
    destinations.get(file).map(_ => Success(FileRecord(r.metadataValue("hydra-file-stream").get, r.payload)))
      .getOrElse(Failure(new IllegalArgumentException(s"No file stream with id $file was configured.")))
  }
}