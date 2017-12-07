package hydra.core.app

import akka.actor.Props
import akka.testkit.TestKit
import com.github.vonnagy.service.container.service.ContainerService
import hydra.core.test.DummyActor
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

class HydraAppSpec extends Matchers with FunSpecLike with BeforeAndAfterAll with ServiceLoader {

   val container = buildContainer()

  override def afterAll = {
    TestKit.shutdownActorSystem(container.system)
    container.shutdown()
  }

  describe("The Hydra app") {
    it("builds a container") {
      val csvc = new ContainerService(Seq(classOf[DummyEndpoint]), Nil, Seq("test" -> Props[DummyActor]), Nil,
        "hydra_test")(container.system)
      csvc.name shouldBe container.name
      csvc.registeredRoutes shouldBe container.registeredRoutes
      csvc.name shouldBe container.name
    }
  }
}