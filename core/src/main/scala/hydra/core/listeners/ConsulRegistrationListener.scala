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
    .address(consulSettings.httpAddress)
    .id(consulSettings.serviceId)
    .service(consulSettings.serviceName)
    .port(consulSettings.httpPort)
    .addTags("system:" + consulSettings.serviceName,
      "akka-management-port:" + consulSettings.httpPort).build()

  override def onStartup(container: ContainerService): Unit = {
    if (ConsulRegistrationListener.usingConsul(container.getConfig(None))) {
      val consulSettings = ConsulSettings(container.getConfig(Some(rootConfig)))
      val reg = ImmutableCatalogRegistration.builder()
        .datacenter(consulSettings.dataCenter)
        .service(service(consulSettings))
        .address(consulSettings.httpAddress)
        .node(consulSettings.nodeName)
        .check(check(consulSettings))
        .build()

      val consul = Consul.builder()
        .withHostAndPort(HostAndPort.fromParts(consulSettings.consulHost, consulSettings.consulPort))
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
        .withHostAndPort(HostAndPort.fromParts(consulSettings.consulHost, consulSettings.consulPort))
        .build()
      consul.catalogClient().deregister(dreg)
      consul.destroy()
    }
  }
}

case class ConsulSettings(config: Config) {

  private val consulConfig = config.getConfig("akka.discovery.akka-consul")
    .withFallback(config.getConfig("consul"))


  val consulHost = consulConfig.getString("consul-host")
  val consulPort = consulConfig.getInt("consul-port")
  val httpAddress = consulConfig.getString("http.address")
  val dataCenter = consulConfig.getString("datacenter")
  val serviceId = consulConfig.getString("service.id")
  val serviceName = consulConfig.getString("service.name")
  val httpPort = consulConfig.getInt("http.port")
  val nodeName = consulConfig.getString("node.name")
  val containerPort = config.getInt("container.http.port")

  val healthEndpoint = Uri.from(scheme = "http",
    host = httpAddress, port = containerPort, path = "/health")
}

object ConsulRegistrationListener {
  def usingConsul(config: Config) =
    config.get[String]("akka.discovery.method")
      .map(method => method.equals("akka-consul")).valueOrElse(false)
}
