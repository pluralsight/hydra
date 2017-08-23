package hydra.core.extension

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.{Config, ConfigFactory}
import hydra.core.extensions._
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

class HydraExtensionSpec extends TestKit(ActorSystem("test"))
  with Matchers with FunSpecLike with BeforeAndAfterAll {

  override def afterAll() = TestKit.shutdownActorSystem(system)

  val cfg = ConfigFactory.parseString(
    """
      |tester{
      |  name=typed-test
      |  class=hydra.core.extension.HydraTestExtension
      |}
    """.stripMargin)

  describe("Hydra extensions") {
    it("can be loaded from configuration") {
      println(HydraExtensionLoader.load("tester", cfg))
    }
  }
}

class TestTypedModule extends HydraTypedModule {
  var counter = 0

  override def id: String = "test-typed"

  override def config: Config = ConfigFactory.empty()

  override def run(): Unit = counter += 1
}


class TestActorModule extends HydraActorModule {
  var counter = 0

  override def id: String = "test"

  override def config: Config = ConfigFactory.empty()

  override def run(): Unit = counter += 1
}
