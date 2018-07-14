package hydra.core.listeners

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.github.vonnagy.service.container.service.ContainerService
import com.google.common.net.HostAndPort
import com.orbitz.consul.Consul
import com.pszymczyk.consul.ConsulStarterBuilder
import com.typesafe.config.ConfigFactory
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.collection.JavaConverters._

class ConsulRegistrationListenerSpec extends TestKit(ActorSystem("ConsulRegistrationListenerSpec"))
  with Matchers
  with FlatSpecLike
  with BeforeAndAfterAll
  with LoggingAdapter
  with ConfigSupport {

  private val consulPort = rootConfig.getInt("consul.http.port")
  private val consulAddress = rootConfig.getString("consul.http.address")
  lazy val consul = ConsulStarterBuilder.consulStarter()
    .withHttpPort(consulPort).build().start()

  override def beforeAll() = log.debug(s"Started consul at ${consul.getAddress}:${consul.getHttpPort}.")

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
    consul.close()
  }

  private lazy val consulClient = Consul.builder()
    .withHostAndPort(HostAndPort.fromParts(consulAddress, consulPort)).build()

  "The ConsulRegistrationListener" should "register Hydra on start up and deregister on shutdown" in {
    val listener = new ConsulRegistrationListener()
    val container = new ContainerService(name = "test")(system)
    listener.onStartup(container)
    val r = consulClient.catalogClient().getService("hydra")
    r.getResponse.size() shouldBe 1
    val hydra = r.getResponse.asScala(0)
    hydra.getAddress shouldBe "localhost"
    hydra.getDatacenter.get shouldBe "dc1"
    hydra.getServiceName shouldBe "hydra"
    hydra.getServiceId shouldBe "hydra"
    hydra.getNode shouldBe "hydra-ingest"
    hydra.getServicePort shouldBe 8500
    hydra.getServiceTags.asScala should contain allOf("system:hydra", "akka-management-port:8500")

    listener.onShutdown(container)
    val dr = consulClient.catalogClient().getService("hydra")
    dr.getResponse.size() shouldBe 0
  }

  it should "return the right value for usingConsul" in {
    val cfg = ConfigFactory.parseString("akka.discovery.method=dns").withFallback(rootConfig)
    ConsulRegistrationListener.usingConsul(cfg) shouldBe false

    ConsulRegistrationListener.usingConsul(rootConfig) shouldBe true

    val cfge = ConfigFactory.parseString("akka.discovery.method=\"\"").withFallback(rootConfig)
    ConsulRegistrationListener.usingConsul(cfge) shouldBe false
  }
}
