package hydra.common.util

import hydra.common.testing.DummyActor
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 3/2/17.
  */
class ActorUtilsSpec extends Matchers with FunSpecLike {

  describe("When using ActorUtils") {
    it("names actors correctly") {
      ActorUtils.actorName(classOf[DummyActor]) shouldBe "dummy_actor"
      ActorUtils.actorName[DummyActor] shouldBe "dummy_actor"
    }
  }

}
