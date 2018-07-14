package hydra.core.bootstrap

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.server.Route
import akka.testkit.TestKit
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import com.github.vonnagy.service.container.listener.ContainerLifecycleListener
import com.github.vonnagy.service.container.service.ContainerService
import hydra.core.ingest.TestIngestorDefault
import hydra.core.test.ConsulTestingSupport
import org.scalatest.{FlatSpecLike, Matchers}

import scala.concurrent.ExecutionContext

/**
  * Created by alexsilva on 3/7/17.
  */
class BootstrappingSupportSpec extends Matchers with FlatSpecLike
  with ConsulTestingSupport with BootstrappingSupport {

  val conf =
    """
      |  hydra_test{
      |
      |  endpoints = ["hydra.core.app.DummyEndpoint"]
      |
      | extensions {
      |    dummy {
      |      enabled = true
      |    }
      |  }
      |}
    """.stripMargin


  lazy val container = containerService

  override def afterAll = {
    TestKit.shutdownActorSystem(container.system)
    container.shutdown()
    super.afterAll()
  }

  "The BootstrappingSupport trait" should
    "load endpoints" in {
    endpoints shouldBe Seq(classOf[DummyEndpoint])
  }

  it should "load listeners" in {
    listeners.map(_.getClass) should contain(classOf[DummyListener])
  }

  it should "load service providers" in {
    serviceProviders should contain allOf(DummyServiceProviderObject.getClass, classOf[DummyServiceProvider])
  }

  it should "ask for services from all providers" in {
    services.map(_._1) should contain allOf("test", "test2")
  }

  it should "build a container" in {
    val csvc = new ContainerService(Seq(classOf[DummyEndpoint]), Nil,
      Seq("test" -> Props[TestIngestorDefault], "test2" -> Props[TestIngestorDefault])
      , Seq(new DummyListener), "hydra_test")(container.system)
    csvc.name shouldBe container.name
    csvc.registeredRoutes shouldBe container.registeredRoutes
    csvc.listeners.map(_.getClass) should contain(classOf[DummyListener])
    csvc.name shouldBe container.name
  }
}

private class DummyListener extends ContainerLifecycleListener {
  override def onShutdown(container: ContainerService): Unit = {}

  override def onStartup(container: ContainerService): Unit = {}
}

private class DummyServiceProvider extends ServiceProvider {
  override val services = Seq("test" -> Props[TestIngestorDefault])
}

private object DummyServiceProviderObject extends ServiceProvider {
  override val services = Seq("test2" -> Props[TestIngestorDefault])
}

private class DummyEndpoint(implicit s: ActorSystem, implicit val e: ExecutionContext)
  extends RoutedEndpoints {
  override def route: Route = get {
    complete("DONE")
  }
}