package hydra.common.util

import hydra.common.testing.DummyActor
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike

/**
  * Created by alexsilva on 3/2/17.
  */
class ActorUtilsSpec extends Matchers with AnyFunSpecLike {

  describe("When using ActorUtils") {
    it("names actors correctly") {
      ActorUtils.actorName(classOf[DummyActor]) shouldBe "dummy_actor"
      ActorUtils.actorName[DummyActor] shouldBe "dummy_actor"
    }
  }

}
