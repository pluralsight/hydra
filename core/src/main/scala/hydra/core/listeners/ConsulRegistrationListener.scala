package hydra.core.listeners

import akka.http.scaladsl.model.Uri
import com.github.vonnagy.service.container.listener.ContainerLifecycleListener
import com.github.vonnagy.service.container.service.ContainerService
import com.google.common.net.HostAndPort
import com.orbitz.consul.Consul
import com.orbitz.consul.model.agent.ImmutableCheck
import com.orbitz.consul.model.catalog.{ImmutableCatalogDeregistration, ImmutableCatalogRegistration}
import com.orbitz.consul.model.health.ImmutableService
import com.typesafe.config.Config
import configs.syntax._
import hydra.common.config.ConfigSupport

class ConsulRegistrationListener extends ContainerLifecycleListener with ConfigSupport {

  private def check(consulSettings: ConsulSettings) = ImmutableCheck.builder()
    .interval("10s")
    .id("health-check")
    .name("health check")
    .http(consulSettings.healthEndpoint.toString())
    .build()

  private def service(consulSettings: ConsulSettings) = ImmutableService.builder()
    .address(consulSettings.akkaManagementHostName)
    .id(consulSettings.serviceId)
    .service(consulSettings.serviceName)
    .port(consulSettings.akkaManagementPort)
    .addTags("system:" + consulSettings.serviceName,
      "akka-management-port:" + consulSettings.akkaManagementPort).build()

  override def onStartup(container: ContainerService): Unit = {
    if (ConsulRegistrationListener.usingConsul(container.getConfig(None))) {
      val consulSettings = ConsulSettings(container.getConfig(Some(rootConfig)))
      val reg = ImmutableCatalogRegistration.builder()
        .datacenter(consulSettings.dataCenter)
        .service(service(consulSettings))
        .address(consulSettings.consulHttpHost)
        .node(consulSettings.nodeName)
        .check(check(consulSettings))
        .build()

      val consul = Consul.builder()
        .withHostAndPort(HostAndPort.fromParts(consulSettings.consulHttpHost,
          consulSettings.consulHttpPort))
        .build()

      consul.catalogClient().register(reg)

      consul.destroy()
    }
  }

  override def onShutdown(container: ContainerService): Unit = {
    if (ConsulRegistrationListener.usingConsul(container.getConfig(None))) {
      val consulSettings = ConsulSettings(container.getConfig(Some(rootConfig)))
      val dreg = ImmutableCatalogDeregistration.builder()
        .datacenter(consulSettings.dataCenter)
        .serviceId(consulSettings.serviceId)
        .node(consulSettings.nodeName)
        .build()
      val consul = Consul.builder()
        .withHostAndPort(HostAndPort.fromParts(consulSettings.consulHttpHost, consulSettings.consulHttpPort))
        .build()
      consul.catalogClient().deregister(dreg)
      consul.destroy()
    }
  }
}

case class ConsulSettings(config: Config) {

  private val consulConfig = config.getConfig("consul")

  val akkaManagementPort = config.getInt("akka.management.http.port")
  val akkaManagementHostName = config.getString("akka.management.http.hostname")
  val consulHttpHost = consulConfig.getString("http.host")
  val consulHttpPort = consulConfig.getInt("http.port")
  val dataCenter = consulConfig.getString("datacenter")
  val serviceId = consulConfig.getString("service.id")
  val serviceName = consulConfig.getString("service.name")
  val nodeName = consulConfig.getString("node.name")
  val checkPort = consulConfig.getInt("service.check.port")
  val checkHost = consulConfig.getString("service.check.host")

  val healthEndpoint = Uri.from(scheme = "http",
    host = checkHost, port = checkPort, path = "/health")
}

object ConsulRegistrationListener {
  def usingConsul(config: Config) =
    config.get[String]("akka.discovery.method")
      .map(method => method.equals("akka-consul")).valueOrElse(false)
}
