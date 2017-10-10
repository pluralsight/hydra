package hydra.core.extensions

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestKit
import com.typesafe.config.{Config, ConfigFactory}
import hydra.core.app.HydraEntryPoint
import hydra.core.test.DummyActor
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

class HydraExtensionListenerSpec extends TestKit(ActorSystem("test"))
  with Matchers with FunSpecLike with BeforeAndAfterAll {

  val conf =
    """
      |  hydra_test{
      |  test {
      |    endpoints = ["hydra.core.app.DummyEndpoint"]
      | }
      |}
    """.stripMargin

  val et = new HydraEntryPoint() {
    override def moduleName: String = "test"

    override def config: Config = ConfigFactory.parseString(conf)

    override def services: Seq[(String, Props)] = Seq("test" -> Props[DummyActor])
  }

  val container = et.buildContainer()

  val cfg = ConfigFactory.parseString(
    """
      |test-extension {
      |  name=typed-test
      |  class=hydra.core.extensions.HydraTestExtension
      |}
    """.stripMargin)

  val e = new HydraExtensionListener("test-extension", cfg)

  override def afterAll() = {
    e.onShutdown(container)
    container.shutdown()
    TestKit.shutdownActorSystem(system)
    TestKit.shutdownActorSystem(container.system)
  }

  describe("Hydra Listeners") {
    it("can be loaded from configuration") {
      e.onStartup(container)
      HydraExtensionRegistry(container.system).getModule("test-typed").isDefined shouldBe true
    }

    it("skips loading on empty config") {
      val e = new HydraExtensionListener("test-extension", ConfigFactory.empty())
      e.hasExtensions shouldBe false
      e.onStartup(container)
      e.onShutdown(container)
    }
  }
}