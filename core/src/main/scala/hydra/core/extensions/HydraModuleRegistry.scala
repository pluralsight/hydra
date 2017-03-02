package hydra.core.extensions

import java.util.concurrent.ConcurrentHashMap

import akka.actor.{ ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider }

import scala.collection._
import scala.collection.convert.decorateAsScala._

/**
 * Created by alexsilva on 5/31/16.
 */
class HydraModuleRegistryImpl extends Extension {
  val map: concurrent.Map[String, HydraModuleWrapper] =
    new ConcurrentHashMap[String, HydraModuleWrapper]().asScala

  //This is the operation this Extension provides
  def addModule(module: HydraModuleWrapper) = map.put(module.id, module)

  def getModule(id: String) = map.get(id)

}

object HydraModuleRegistry extends ExtensionId[HydraModuleRegistryImpl] with ExtensionIdProvider {
  override def lookup = HydraModuleRegistry

  override def createExtension(system: ExtendedActorSystem) = new HydraModuleRegistryImpl

  override def get(system: ActorSystem): HydraModuleRegistryImpl = super.get(system)
}
