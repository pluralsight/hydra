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

class ConsulRegistrationContainerListener extends ContainerLifecycleListener with ConfigSupport {

  private val usingConsul = rootConfig.get[String]("akka.discovery.method")
    .map(method => method.equals("akka-consul")).valueOrElse(false)

  private lazy val consulSettings = ConsulSettings(rootConfig)

  private lazy val consul = Consul.builder()
    .withHostAndPort(HostAndPort.fromParts(consulSettings.consulHost, consulSettings.consulPort))
    .build()

  private lazy val check = ImmutableCheck.builder()
    .interval("10s")
    .id("health-check")
    .name("health check")
    .http(consulSettings.healthEndpoint.toString())
    .build()

  private lazy val service = ImmutableService.builder()
    .address(consulSettings.serviceAddress)
    .id(consulSettings.serviceId)
    .service(consulSettings.serviceName)
    .port(consulSettings.servicePort)
    .addTags("system:" + consulSettings.serviceName,
      "akka-management-port:" + consulSettings.servicePort).build()

  override def onStartup(container: ContainerService): Unit = {
    if (usingConsul) {
      val reg = ImmutableCatalogRegistration.builder()
        .datacenter(consulSettings.dataCenter).service(service)
        .address(consulSettings.serviceAddress)
        .node(consulSettings.nodeName)
        .check(check)
        .build()

      consul.catalogClient().register(reg)
    }
  }

  override def onShutdown(container: ContainerService): Unit = {
    if (usingConsul) {
      val dreg = ImmutableCatalogDeregistration.builder()
        .datacenter(consulSettings.dataCenter)
        .serviceId(consulSettings.serviceId)
        .node(consulSettings.nodeName)
        .build()
      consul.catalogClient().deregister(dreg)
    }
  }
}

case class ConsulSettings(config: Config) {

  private val consulConfig = config.getConfig("akka.discovery.akka-consul")
    .withFallback(config.getConfig("consul"))


  val consulHost = consulConfig.getString("consul-host")
  val consulPort = consulConfig.getInt("consul-port")
  val serviceAddress = consulConfig.getString("service.address")
  val dataCenter = consulConfig.getString("datacenter")
  val serviceId = consulConfig.getString("service.id")
  val serviceName = consulConfig.getString("service.name")
  val servicePort = consulConfig.getInt("service.port")
  val nodeName = consulConfig.getString("node.name")
  val containerPort = config.getInt("container.http.port")

  val healthEndpoint = Uri.from(scheme = "http",
    host = serviceAddress, port = containerPort, path = "/health")
}
