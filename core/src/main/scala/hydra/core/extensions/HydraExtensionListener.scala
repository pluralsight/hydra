package hydra.core.extensions

import com.github.vonnagy.service.container.listener.ContainerLifecycleListener
import com.github.vonnagy.service.container.service.ContainerService
import com.typesafe.config.Config
import hydra.common.logging.LoggingAdapter

/**
  * Waits for the main actor system to be available before starting the extensions.
  * Created by alexsilva on 2/1/16.
  *
  */
class HydraExtensionListener(extensionName: String, config: Config) extends ContainerLifecycleListener
  with LoggingAdapter {

  private[extensions] val hasExtensions = !config.isEmpty

  override def onStartup(container: ContainerService) = {
    if (hasExtensions) HydraExtensionLoader.load(extensionName, config)(container.system)
  }

  override def onShutdown(container: ContainerService): Unit = {
    if (hasExtensions) HydraExtensionRegistry.get(container.system).getTypedModules.foreach(_.stop())
  }
}
