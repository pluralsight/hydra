package hydra.core.listeners

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.github.vonnagy.service.container.service.ContainerService
import com.typesafe.config.ConfigFactory
import hydra.core.test.ConsulTestingSupport
import org.scalatest.{FlatSpecLike, Matchers}

import scala.collection.JavaConverters._

class ConsulRegistrationListenerSpec extends TestKit(ActorSystem("ConsulRegistrationListenerSpec"))
  with Matchers
  with FlatSpecLike
  with ConsulTestingSupport {

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

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
    hydra.getServicePort shouldBe 8558
    hydra.getServiceTags.asScala should contain allOf("system:hydra", "akka-management-port:8558")

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

  it should "be set up properly from configs" in {
    val config = ConfigFactory.parseString(
      s"""
        |container.http.port = 8080
        |
        |akka {
        |  discovery {
        |    method = akka-consul
        |  }
        |
        |  management {
        |    http {
        |      hostname = "127.0.0.1"
        |      port = 8558
        |      bind-hostname = 0.0.0.0
        |      bind-port = 8558
        |    }
        |  }
        |}
        |
        |consul {
        |  datacenter = dc1
        |  node.name = hydra-node
        |  service {
        |    id = hydra
        |    name = hydra-ingest
        |    check {
        |      host = hydra-test
        |      port = $${container.http.port}
        |    }
        |  }
        |  http {
        |    host = localhost
        |    port = 8500
        |  }
        |}""".stripMargin).resolve()

    val settings = ConsulSettings(config)
    settings.akkaManagementPort shouldBe 8558
    settings.akkaManagementHostName shouldBe "localhost"
    settings.consulHttpPort shouldBe 8500
    settings.dataCenter shouldBe "dc1"
    settings.consulHttpHost shouldBe "localhost"
    settings.serviceName shouldBe "hydra-ingest"
    settings.serviceId shouldBe "hydra"
    settings.nodeName shouldBe "hydra-node"
    settings.healthEndpoint.toString shouldBe "http://hydra-test:8080/health"
  }
}
