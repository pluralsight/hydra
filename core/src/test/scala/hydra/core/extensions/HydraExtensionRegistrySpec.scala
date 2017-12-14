package hydra.core.extensions

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.github.vonnagy.service.container.service.ContainerService
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class HydraExtensionRegistrySpec extends TestKit(ActorSystem("test"))
  with Matchers with FlatSpecLike with BeforeAndAfterAll {


  val cfg = ConfigFactory.parseString(
    """
      |extensions {
      |   test-extension {
      |     name = typed-test
      |     class = hydra.core.extensions.TestTypedExtension
      |   }
      |}
    """.stripMargin)

  val e = HydraExtensionListener(cfg)
  val container = new ContainerService(name = "test")(system)

  override def afterAll() = {
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  "The HydraExtensionRegistry" should "load a typed actor" in {
    HydraExtensionRegistry(system).register("test", Right(new TestTypedExtension()))
    HydraExtensionRegistry(system).getModule("test").get.toOption.get shouldBe a[TestTypedExtension]
    HydraExtensionRegistry(system).getTypedModules(0) shouldBe a[TestTypedExtension]
  }

  class TestTypedExtension extends HydraTypedModule {
    val id = "test"
    val config = ConfigFactory.empty()
    var counter = 0

    def run() = counter += 1
  }

}