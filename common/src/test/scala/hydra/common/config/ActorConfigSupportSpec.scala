package hydra.common.config

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit}
import hydra.common.testing.DummyActor
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 3/2/17.
  */
class ActorConfigSupportSpec extends TestKit(ActorSystem("test")) with Matchers with FunSpecLike with ConfigSupport {

  val dummy = TestActorRef[DummyActor]

  describe("When mixing the trait in an actor") {
    it("has the correct actor name") {
      dummy.underlyingActor.thisActorName shouldBe "dummy_actor"
    }

    it("has the correct actor config") {
      dummy.underlyingActor.actorConfig shouldBe rootConfig.getConfig("hydraTest.actors.dummy_actor")
    }

  }
}
