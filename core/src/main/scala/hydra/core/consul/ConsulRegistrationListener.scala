package hydra.core.consul

import com.github.vonnagy.service.container.listener.ContainerLifecycleListener
import com.github.vonnagy.service.container.service.ContainerService
import com.typesafe.config.Config
import configs.syntax._
import hydra.common.config.ConfigSupport

class ConsulRegistrationListener extends ContainerLifecycleListener with ConfigSupport {


  override def onStartup(container: ContainerService): Unit = {
    if (ConsulRegistrationListener.usingConsul(container.getConfig(None))) {
      val consulSettings = ConsulSettings(container.getConfig(Some(rootConfig)))
      ConsulRegistration.register(consulSettings, container.system.name)
    }
  }

  override def onShutdown(container: ContainerService): Unit = {
    if (ConsulRegistrationListener.usingConsul(container.getConfig(None))) {
      val consulSettings = ConsulSettings(container.getConfig(Some(rootConfig)))
      ConsulRegistration.unregister(consulSettings)
    }
  }
}

object ConsulRegistrationListener {
  def usingConsul(config: Config) =
    config.get[String]("akka.discovery.method")
      .map(method => method.equals("akka-consul")).valueOrElse(false)
}
