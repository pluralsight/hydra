package hydra.core.extensions

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.github.vonnagy.service.container.service.ContainerService
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

class HydraExtensionListenerSpec extends TestKit(ActorSystem("test"))
  with Matchers with FunSpecLike with BeforeAndAfterAll   {

  val conf =
    """
      |  hydra_test{
      |  test {
      |    endpoints = ["hydra.core.app.DummyEndpoint"]
      | }
      |}
    """.stripMargin

  val cfg = ConfigFactory.parseString(
    """
      |extensions {
      |   test-extension {
      |     name = typed-test
      |     class = hydra.core.extensions.HydraTestExtension
      |   }
      |}
    """.stripMargin)

  val e = HydraExtensionListener(cfg)
  val container = new ContainerService(name="test")(system)

  override def afterAll() = {
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  describe("Hydra Listeners") {
    it("can be loaded from configuration") {
      e.onStartup(container)
      HydraExtensionRegistry(system).getModule("test-typed").isDefined shouldBe true
    }

    it("skips loading on empty config") {
      val e = new HydraExtensionListener(ConfigFactory.empty())
      e.hasExtensions shouldBe false
      e.onStartup(container)
      e.onShutdown(container)
    }
  }
}