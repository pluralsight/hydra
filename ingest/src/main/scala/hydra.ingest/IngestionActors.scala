package hydra.ingest

import akka.actor.Props
import hydra.common.util.ActorUtils
import hydra.ingest.services.{IngestionActor, IngestorRegistrar, IngestorRegistry}

/**
  * Created by alexsilva on 3/29/17.
  */
trait IngestionActors {

  val services = Seq(
    Tuple2(ActorUtils.actorName[IngestorRegistry], Props[IngestorRegistry]),
    Tuple2(ActorUtils.actorName[IngestorRegistrar], Props[IngestorRegistrar]),
    Tuple2(ActorUtils.actorName[IngestionActor], Props[IngestionActor]))
}
