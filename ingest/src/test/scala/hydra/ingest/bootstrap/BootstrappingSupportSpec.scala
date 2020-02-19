package hydra.ingest.bootstrap

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.server.Route
import akka.testkit.TestKit
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import com.github.vonnagy.service.container.listener.ContainerLifecycleListener
import com.github.vonnagy.service.container.service.ContainerService
import hydra.core.bootstrap.ServiceProvider
import hydra.core.ingest.{HydraRequest, Ingestor}
import hydra.core.transport.{AckStrategy, HydraRecord, RecordFactory}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

class TestIngestorDefault extends Ingestor {


  /**
    * This will _not_ override; instead it will use the default value of 1.second. We'll test it.
    */
  override val initTimeout = 2.millisecond

  val to = context.receiveTimeout

  ingest {
    case "hello" => sender ! "hi!"
    case "timeout" => sender ! to
  }

  override val recordFactory = TestRecordFactory
}

object TestRecordFactory extends RecordFactory[String, String] {
  override def build(r: HydraRequest)(implicit ec: ExecutionContext) = {
    val timeout = r.metadataValueEquals("timeout", "true")
    if (timeout) {
      Future.successful(TimeoutRecord("test-topic", r.correlationId.toString, r.payload,
        r.ackStrategy))
    }
    else {
      Future.successful(TestRecord("test-topic", r.correlationId.toString, r.payload,
        r.ackStrategy))
    }
  }
}

case class TestRecord(destination: String,
                      key: String,
                      payload: String,
                      ackStrategy: AckStrategy) extends HydraRecord[String, String]


case class TimeoutRecord(destination: String,
                         key: String,
                         payload: String,
                         ackStrategy: AckStrategy) extends HydraRecord[String, String]

class BootstrappingSupportSpec extends Matchers with FlatSpecLike with BootstrappingSupport with BeforeAndAfterAll {

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
  }

  "The BootstrappingSupport trait" should
    "load endpoints" in {
    endpoints should contain (classOf[DummyEndpoint])
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