package hydra.core.extension

import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import com.typesafe.config.{Config, ConfigFactory}
import hydra.core.extensions.HydraExtensionBase

case class HydraTestExtensionImpl(system: ActorSystem, cfg: Config)
  extends HydraExtensionBase("test-ext", cfg)(system) with Extension

object HydraTestExtension extends ExtensionId[HydraTestExtensionImpl] with ExtensionIdProvider {
  val cfg = ConfigFactory.parseString(
    """
      |tester{
      |  name=typed-test
      |  class=hydra.core.extension.HydraTestExtension
      |}
    """.stripMargin)

  override def lookup = HydraTestExtension

  override def createExtension(system: ExtendedActorSystem) = new HydraTestExtensionImpl(system, cfg)

  override def get(system: ActorSystem): HydraTestExtensionImpl = super.get(system)
}