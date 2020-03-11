package hydra.ingest.bootstrap

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import hydra.common.util.ActorUtils
import hydra.core.bootstrap.ReflectionsWrapper
import hydra.ingest.IngestorInfo
import hydra.ingest.services.IngestorRegistry
import hydra.ingest.services.IngestorRegistry.RegisterWithClass
import hydra.ingest.test.TestIngestor
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.duration._

class HydraIngestorRegistrySpec
    extends TestKit(ActorSystem("HydraIngestorRegistrySpec"))
    with Matchers
    with AnyFunSpecLike
    with BeforeAndAfterAll
    with ImplicitSender
    with ScalaFutures {

  override def afterAll =
    TestKit.shutdownActorSystem(
      system,
      verifySystemShutdown = true,
      duration = 10.seconds
    )

  val testRegistry =
    system.actorOf(Props[IngestorRegistry], "ingestor_registry")

  val cfg = ConfigFactory.parseString(
    "ingest.ingestor-registry.path=/user/ingestor_registry"
  )
  val registry = HydraIngestorRegistryClient(cfg)

  implicit val actorRefFactory = system

  ReflectionsWrapper.rescan()

  registry.registry ! RegisterWithClass(classOf[TestIngestor], "global")
  expectMsgType[IngestorInfo]

  describe("The Ingestor Registry") {
    it("uses the default registry if no config") {
      val path = HydraIngestorRegistryClient.registryPath(ConfigFactory.empty())
      path shouldBe s"/user/service/${ActorUtils.actorName(classOf[IngestorRegistry])}"
    }

    it("looks up an ingestor") {
      implicit val timeout = akka.util.Timeout(10.seconds)
      whenReady(registry.lookupIngestor("test_ingestor")) { i =>
        i.ingestors.size shouldBe 1
        i.ingestors(0).name shouldBe "test_ingestor"
        i.ingestors(0).path shouldBe testRegistry.path / "test_ingestor"
      }
    }
  }
}
