package hydra.core.extensions

import com.github.vonnagy.service.container.listener.ContainerLifecycleListener
import com.github.vonnagy.service.container.service.ContainerService
import com.typesafe.config.{Config, ConfigFactory}
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import configs.syntax._

/**
  * Waits for the main actor system to be available before starting the extensions.
  * Created by alexsilva on 2/1/16.
  *
  */
class HydraExtensionListener(config: Config) extends ContainerLifecycleListener
  with LoggingAdapter with ConfigSupport {

  private lazy val extensions = config.getOrElse[Config]("extensions", ConfigFactory.empty).value

  private[extensions] val hasExtensions = !extensions.isEmpty

  override def onStartup(container: ContainerService): Unit = {
    if (hasExtensions) HydraExtensionLoader.load(extensions)(container.system)
  }

  override def onShutdown(container: ContainerService): Unit = {
    if (hasExtensions) HydraExtensionRegistry.get(container.system).getTypedModules.foreach(_.stop())
  }
}

object HydraExtensionListener {
  def apply(config: Config): HydraExtensionListener = new HydraExtensionListener(config)
}
