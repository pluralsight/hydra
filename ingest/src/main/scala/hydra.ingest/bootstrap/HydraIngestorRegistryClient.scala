package hydra.ingest.bootstrap

import akka.actor.{ActorSelection, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import hydra.common.util.ActorUtils
import hydra.ingest.services.IngestorRegistry
import hydra.ingest.services.IngestorRegistry.{FindByName, LookupResult}

import scala.concurrent.Future

/**
  * Created by alexsilva on 2/21/17.
  */
class HydraIngestorRegistryClient(registryPath: String)(
    implicit val system: ActorSystem
) {

  lazy val registry: ActorSelection = system.actorSelection(registryPath)

  def lookupIngestor(
      name: String
  )(implicit timeout: Timeout): Future[LookupResult] = {
    (registry ? FindByName(name)).mapTo[LookupResult]
  }
}

object HydraIngestorRegistryClient {

  import hydra.common.config.ConfigSupport._

  def registryPath(config: Config) =
    config
      .getStringOpt("ingest.ingestor-registry.path")
      .getOrElse(
        s"/user/service/${ActorUtils.actorName(classOf[IngestorRegistry])}"
      )

  def apply(
      config: Config
  )(implicit system: ActorSystem): HydraIngestorRegistryClient = {
    new HydraIngestorRegistryClient(registryPath(config))(system)
  }
}
