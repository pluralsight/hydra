package hydra.core.extensions

import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import com.typesafe.config.{Config, ConfigFactory}
import hydra.core.extensions.HydraActorModule.Run

case class HydraTestExtensionImpl(system: ActorSystem, cfg: Config)
  extends HydraExtensionBase("tester", cfg)(system) with Extension

object HydraTestExtension extends ExtensionId[HydraTestExtensionImpl] with ExtensionIdProvider {

  val cfg = ConfigFactory.parseString(
    """
      |  test-typed {
      |    name=typed-module-test
      |    class=hydra.core.extensions.TestTypedModule
      |}
      |
      | test-typed-interval {
      |    name=typed-module-test
      |    class=hydra.core.extensions.TestTypedModule
      |    interval = 50ms
      |}
      |
      |  test-actor {
      |    name=typed-actor-test
      |    class=hydra.core.extensions.TestActorModule
      |}
      |  test-actor-disabled {
      |    enabled=false
      |    name=typed-actor-test
      |    class=hydra.core.extensions.TestActorModule
      |}
    """.stripMargin)

  override def lookup = HydraTestExtension

  override def createExtension(system: ExtendedActorSystem) = new HydraTestExtensionImpl(system, cfg)

  override def get(system: ActorSystem): HydraTestExtensionImpl = super.get(system)
}

class TestTypedModule(val id: String, val config: Config) extends HydraTypedModule {
  override def run(): Unit = TestTypedModuleCounter.counter += 1
}


class TestActorModule(val id: String, val config: Config) extends HydraActorModule {
  @volatile
  var counter = 0

  override def run(): Unit = counter += 1

  override def receive = synchronized {
    case Run => run()
    case "counter" => sender ! counter
  }
}

object TestTypedModuleCounter {
  @volatile
  var counter = 0
}
