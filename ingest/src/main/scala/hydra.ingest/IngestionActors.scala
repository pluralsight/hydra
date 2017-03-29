package hydra.ingest

import akka.actor.Props
import hydra.common.util.ActorUtils
import hydra.ingest.services.{IngestionActor, IngestionErrorHandler, IngestorRegistrar, IngestorRegistry}

/**
  * Created by alexsilva on 3/29/17.
  */
trait IngestionActors {

  val services = Seq(
    Tuple2(ActorUtils.actorName[IngestorRegistry], Props[IngestorRegistry]),
    Tuple2(ActorUtils.actorName[IngestorRegistrar], Props[IngestorRegistrar]),
    Tuple2(ActorUtils.actorName[IngestionErrorHandler], Props[IngestionErrorHandler]),
    Tuple2(ActorUtils.actorName[IngestionActor], Props(classOf[IngestionActor], "/user/service/ingestor_registry")))
}
