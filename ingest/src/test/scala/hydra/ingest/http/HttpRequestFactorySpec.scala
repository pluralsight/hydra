package hydra.ingest.http

import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.server.RequestContext
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 3/17/17.
  */
class HttpRequestFactorySpec extends Matchers with FunSpecLike with MockFactory {
  describe("When build a HydraRequest from HTTP") {
    it("builds") {
      val httpRequest = stub[HttpRequest]
      (httpRequest.headers _)
      val req = new HttpRequestFactory().createRequest(Some("label"), "Payload", ctx)
      req.payload shouldBe "payload"
      req.label shouldBe Some("label")
      req.metadataValue("test") shouldBe Some("test")
    }
  }

}
