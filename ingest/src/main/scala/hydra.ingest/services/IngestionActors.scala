package hydra.ingest.services

import akka.actor.Props
import hydra.common.util.ActorUtils
import hydra.core.bootstrap.ServiceProvider

/**
  * Created by alexsilva on 3/29/17.
  */
object IngestionActors extends ServiceProvider {

  override val services = Seq(
    Tuple2(ActorUtils.actorName[TransportRegistrar], Props[TransportRegistrar]),
    Tuple2(ActorUtils.actorName[IngestorRegistry], Props[IngestorRegistry]),
    Tuple2(ActorUtils.actorName[IngestorRegistrar], Props[IngestorRegistrar]),
    Tuple2(ActorUtils.actorName[IngestionActor], Props[IngestionActor]))
}
