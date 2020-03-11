package hydra.common.config

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit}
import hydra.common.testing.DummyActor
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.BeforeAndAfterAll

/**
  * Created by alexsilva on 3/2/17.
  */
class ActorConfigSupportSpec
    extends TestKit(ActorSystem("test"))
    with Matchers
    with AnyFunSpecLike
    with BeforeAndAfterAll
    with ConfigSupport {

  val dummy = TestActorRef[DummyActor]

  override def afterAll = TestKit.shutdownActorSystem(system)

  describe("When mixing the trait in an actor") {
    it("has the correct actor name") {
      dummy.underlyingActor.thisActorName shouldBe "dummy_actor"
    }

    it("has the correct actor config") {
      dummy.underlyingActor.actorConfig shouldBe rootConfig.getConfig(
        "hydraTest.actors.dummy_actor"
      )
    }

  }
}
