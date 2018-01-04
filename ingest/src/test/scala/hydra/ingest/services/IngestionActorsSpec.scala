package hydra.ingest.services

import hydra.common.util.ActorUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

/**
  * Created by alexsilva on 3/29/17.
  */
class IngestionActorsSpec extends Matchers with FlatSpecLike with BeforeAndAfterAll {

  "The ingestion actors sequence" should "contain all actors" in {
    IngestionActors.services.map(_._1) shouldBe Seq(
      ActorUtils.actorName[TransportRegistrar],
      ActorUtils.actorName[IngestorRegistry],
      ActorUtils.actorName[IngestorRegistrar])
  }
}
