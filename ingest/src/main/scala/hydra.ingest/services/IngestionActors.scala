package hydra.ingest.services

import akka.actor.Props
import hydra.common.config.ConfigSupport
import hydra.common.util.ActorUtils
import hydra.core.bootstrap.ServiceProvider
import hydra.ingest.bootstrap.HydraIngestorRegistryClient

/**
  * Created by alexsilva on 3/29/17.
  */
object IngestionActors extends ServiceProvider with ConfigSupport {

  private val registryPath =
    HydraIngestorRegistryClient.registryPath(applicationConfig)

  override val services = Seq(
    Tuple2(
      ActorUtils.actorName[IngestionHandlerGateway],
      IngestionHandlerGateway.props(registryPath)
    ),
    Tuple2(ActorUtils.actorName[TransportRegistrar], Props[TransportRegistrar]),
    Tuple2(ActorUtils.actorName[IngestorRegistry], Props[IngestorRegistry]),
    Tuple2(ActorUtils.actorName[IngestorRegistrar], Props[IngestorRegistrar])
  )
}
