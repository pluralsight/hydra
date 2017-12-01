package hydra.core.extensions

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, ExtensionId, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import hydra.core.extensions.HydraActorModule.Run
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.util.Try

class HydraExtensionSpec extends TestKit(ActorSystem("test"))
  with Matchers with FunSpecLike with BeforeAndAfterAll with ImplicitSender {

  import akka.testkit.TestKit
  import com.typesafe.config.ConfigFactory

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system, verifySystemShutdown = true)

  val probe = TestProbe()

  val testerActor = TestActorRef[ExtTestActor](Props(new ExtTestActor(probe.ref)), "ext-test")

  val cfg = ConfigFactory.parseString(
    """
      |  extensions {
      |    test-extension {
      |      enabled = true
      |      class = hydra.core.extensions.HydraTestExtension
      |    }
      |  }
    """.stripMargin)

  describe("Hydra extensions") {
    it("can be loaded from configuration") {
      val ext: Seq[Try[ExtensionId[_]]] = HydraExtensionLoader.load(cfg)
      ext(0).get.asInstanceOf[ExtensionId[HydraTestExtensionImpl]].get(system).extName shouldBe "tester"
    }

    it("reports failure") {
      val cfg = ConfigFactory.parseString(
        """
          |    extensions {
          |      test-extension-disabled {
          |      enabled = false
          |      class = hydra.core.extensions.HydraTestExtension
          |    }
          |  }
        """.stripMargin)
      val ext: Seq[Try[ExtensionId[_]]] = HydraExtensionLoader.load(cfg)
      intercept[IllegalArgumentException] {
        ext(0).get
      }
    }

    it("register modules with the extension registry") {
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

      probe.expectMsg("called")
    }

    it("calls the run method on typed modules") {
      val act: HydraTypedModule = HydraExtensionRegistry(system).getModule("test-typed").get
        .asInstanceOf[Either[ActorRef, HydraTypedModule]].right.toOption.get
      act.run()
      probe.expectMsg("called")
    }
  }
}

class ExtTestActor(probe: ActorRef) extends Actor with ActorLogging {
  override def receive = {
    case Run => probe ! "called"
  }
}


