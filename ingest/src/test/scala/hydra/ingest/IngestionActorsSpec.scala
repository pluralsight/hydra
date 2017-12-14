package hydra.ingest

import hydra.common.util.ActorUtils
import hydra.ingest.services._
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

/**
  * Created by alexsilva on 3/29/17.
  */
class IngestionActorsSpec extends Matchers with FlatSpecLike with BeforeAndAfterAll {

  "The ingestion actors sequence" should "contain all actors" in {
    IngestionActors.services.map(_._1) shouldBe Seq(
      ActorUtils.actorName[TransportRegistrar],
      ActorUtils.actorName[IngestorRegistry],
      ActorUtils.actorName[IngestorRegistrar],
      ActorUtils.actorName[IngestionActor])
  }
}
