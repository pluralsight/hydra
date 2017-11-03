package hydra.core.extensions

import akka.actor.{ActorRef, ActorSystem, ExtensionId}
import akka.parboiled2.RuleTrace.Run
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

class HydraExtensionSpec extends TestKit(ActorSystem("test"))
  with Matchers with FunSpecLike with BeforeAndAfterAll with ImplicitSender with Eventually {

  import akka.testkit.TestKit
  import com.typesafe.config.ConfigFactory

  override def afterAll() = TestKit.shutdownActorSystem(system)

  val cfg = ConfigFactory.parseString(
    """
      |test-extension {
      |tester{
      |  name=typed-test
      |  class=hydra.core.extensions.HydraTestExtension
      |}
      |}
    """.stripMargin)

  describe("Hydra extensions") {
    ignore("can be loaded from configuration") {

      val cfg = ConfigFactory.parseString(
        """
          |test-extension {
          |tester{
          |  name=typed-test
          |  class=hydra.core.extensions.HydraTestExtension
          |}
          |}
        """.stripMargin)
      val ext = HydraExtensionLoader.load("test-extension", cfg.getConfig("tester").atKey("tester"))
      ext.get.asInstanceOf[ExtensionId[HydraTestExtensionImpl]].get(system).extName shouldBe "tester"
    }

    ignore("register modules with the extension registry") {
      HydraExtensionRegistry(system).getModule("test-typed").isDefined shouldBe true
      HydraExtensionRegistry(system).getModule("test-actor").isDefined shouldBe true
      HydraExtensionRegistry.get(system).getModule("test-actor-disabled").isDefined shouldBe false
    }

    it("errors with a module with the same name exists") {
      HydraTestExtension(system).registerModule("test-typed", ConfigFactory.empty)
    }

    it("calls the run method on actor modules") {
      val act: ActorRef = HydraExtensionRegistry(system).getModule("test-actor").get
        .asInstanceOf[Either[ActorRef, HydraTypedModule]].left.toOption.get
      act ! Run

      Thread.sleep(1000)
      eventually {
        act ! "counter"
        expectMsg(1)
      }
    }

    it("calls the run method on typed modules") {
      val act: HydraTypedModule = HydraExtensionRegistry(system).getModule("test-typed").get
        .asInstanceOf[Either[ActorRef, HydraTypedModule]].right.toOption.get
      TestTypedModuleCounter.counter = 0
      act.run()
      eventually {
        TestTypedModuleCounter.counter shouldBe 1
      }
    }
  }
}
