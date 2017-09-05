package hydra.ingest

import hydra.common.util.ActorUtils
import hydra.ingest.services.{IngestionActor, IngestorRegistrar, IngestorRegistry}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

/**
  * Created by alexsilva on 3/29/17.
  */
class IngestionActorsSpec extends Matchers with FlatSpecLike with BeforeAndAfterAll with IngestionActors {

  "The ingestion actors sequence" should "contain all actors" in {
    services.map(_._1) shouldBe Seq(
      ActorUtils.actorName[IngestorRegistry],
      ActorUtils.actorName[IngestorRegistrar],
      ActorUtils.actorName[IngestionActor])
  }
}
