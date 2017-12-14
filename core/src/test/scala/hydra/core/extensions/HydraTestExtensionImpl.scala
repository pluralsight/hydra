package hydra.core.extensions

import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, TypedActor}
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
      |    actorPath = "/user/ext-test"
      |}
      |
      | test-typed-interval {
      |    name=typed-module-test
      |    class=hydra.core.extensions.TestTypedModule
      |    actorPath = "/user/ext-test"
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

  override def run(): Unit = {
    val act = TypedActor.context.actorSelection(config.getString("actorPath"))
    act ! Run
  }

  override def stop(): Unit = {
    val act = TypedActor.context.actorSelection(config.getString("actorPath"))
    act ! "stopped"
  }
}


class TestActorModule(val id: String, val config: Config) extends HydraActorModule {

  import configs.syntax._

  val act = context.actorSelection(config.getOrElse[String]("actorPath", "").value)

  override def run(): Unit = act ! Run

  override def receive = {
    case Run => run()
  }
}

