package hydra.ingest.bootstrap

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

class RequestFactoriesSpec extends TestKit(ActorSystem("test")) with Matchers with FunSpecLike
  with BeforeAndAfterAll with ScalaFutures {
  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  import RequestFactories._

  private implicit val mat = ActorMaterializer()

  describe("The RequestFactories") {
    it("build a Hydra request from an HTTP request") {
      val hr = HttpRequest(entity = "test")
      val hydraReq = createRequest(1, hr)
      whenReady(hydraReq) { r =>
        r.payload shouldBe "test"
      }
    }
  }
}
