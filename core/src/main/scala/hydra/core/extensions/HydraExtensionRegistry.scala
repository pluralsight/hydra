package hydra.core.extensions

import akka.actor.{ActorRef, ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}

import scala.collection._

/**
  * Created by alexsilva on 5/31/16.
  */
class HydraExtensionRegistryImpl extends Extension {
  type EXT = Either[ActorRef, HydraTypedExtension]

  private val map: concurrent.Map[String, EXT] = new scala.collection.concurrent.TrieMap[String, EXT]()

  def register(id: String, ext: EXT) = map.put(id, ext)

  def getModule(id: String): Option[EXT] = map.get(id)

}

object HydraExtensionRegistry extends ExtensionId[HydraExtensionRegistryImpl] with ExtensionIdProvider {
  override def lookup = HydraExtensionRegistry

  override def createExtension(system: ExtendedActorSystem) = new HydraExtensionRegistryImpl

  override def get(system: ActorSystem): HydraExtensionRegistryImpl = super.get(system)
}
