package hydra.ingest.request

import akka.actor.Actor
import hydra.core.ingest.{HydraRequest, IngestionParams}
import hydra.ingest.bootstrap.HydraIngestorRegistry
import hydra.ingest.request.IngestionHandler.Initiate
import hydra.ingest.services.IngestorRegistry.{FindAll, FindByName}

/**
  * Created by alexsilva on 3/27/17.
  */
class IngestionHandler extends Actor with HydraIngestorRegistry {

  implicit val system = context.system

  override def receive: Receive = {
    case Initiate(request) =>
      ingestorRegistry.foreach { registry =>
        request.metadataValue(IngestionParams.HYDRA_INGESTOR_TARGET_PARAM) match {
          case Some(ingestor) => registry ! FindByName(ingestor)
          case None => registry ! FindAll
        }
      }
  }

}

object IngestionHandler {

  case class Initiate(request: HydraRequest)

}
