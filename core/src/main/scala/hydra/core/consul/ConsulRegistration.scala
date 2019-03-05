package hydra.core.consul

import akka.http.scaladsl.model.Uri
import com.google.common.net.HostAndPort
import com.orbitz.consul.Consul
import com.orbitz.consul.model.agent.ImmutableCheck
import com.orbitz.consul.model.catalog.{ImmutableCatalogDeregistration, ImmutableCatalogRegistration}
import com.orbitz.consul.model.health.ImmutableService
import com.typesafe.config.Config

object ConsulRegistration {

  private def check(consulSettings: ConsulSettings) = ImmutableCheck.builder()
    .interval("10s")
    .id(s"${consulSettings.serviceId}-health-check")
    .serviceId(consulSettings.serviceId)
    .name(s"Health endpoint on ${consulSettings.healthEndpoint}")
    .http(consulSettings.healthEndpoint.toString)
    .build()

  def createService(consulSettings: ConsulSettings, systemName: String) = ImmutableService.builder()
    .address(consulSettings.akkaManagementHostName)
    .id(consulSettings.serviceId)
    .service(consulSettings.serviceName)
    .port(consulSettings.akkaManagementPort)
    .addTags(s"system:$systemName",
      "akka-management-port:" + consulSettings.akkaManagementPort).build()

  def register(consulSettings: ConsulSettings, systemName: String): Unit = {
    val reg = ImmutableCatalogRegistration.builder()
      .datacenter(consulSettings.dataCenter)
      .service(createService(consulSettings, systemName))
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

  def unregister(consulSettings: ConsulSettings): Unit = {
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