package hydra.ingest.bootstrap

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import hydra.ingest.ingestors.IngestorInfo
import hydra.ingest.services.IngestorRegistry
import hydra.ingest.services.IngestorRegistry.RegisterWithClass
import hydra.ingest.test.TestIngestor
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

class HydraIngestorRegistrySpec extends TestKit(ActorSystem("test")) with Matchers
  with FunSpecLike with HydraIngestorRegistry with BeforeAndAfterAll with ImplicitSender with ScalaFutures {

  override def afterAll = TestKit.shutdownActorSystem(system)

  val registry = system.actorOf(Props[IngestorRegistry], "ingestor_registry")

  registry ! RegisterWithClass(classOf[TestIngestor], "global")
  expectMsgType[IngestorInfo]

  describe("The Ingestor Registry") {
    it("looks up an ingestor") {
      whenReady(lookupIngestor("test_ingestor")) { i =>
        i.ingestors.size shouldBe 1
        i.ingestors(0).name shouldBe "test_ingestor"
        i.ingestors(0).path shouldBe registry.path / "test_ingestor"
      }
    }
  }
}
