package hydra.ingest.bootstrap

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.duration._

class RequestFactoriesSpec
    extends TestKit(ActorSystem("RequestFactoriesSpec"))
    with Matchers
    with AnyFunSpecLike
    with BeforeAndAfterAll
    with ScalaFutures {

  override def afterAll =
    TestKit.shutdownActorSystem(
      system,
      verifySystemShutdown = true,
      duration = 10.seconds
    )

  import RequestFactories._

  describe("The RequestFactories") {
    it("build a Hydra request from an HTTP request") {
      val hr = HttpRequest(entity = "test")
      val hydraReq = createRequest("1", hr)
      whenReady(hydraReq) { r => r.payload shouldBe "test" }
    }
  }
}
