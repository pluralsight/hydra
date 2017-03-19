package hydra.ingest.http

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.ActorMaterializer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpecLike, Matchers}
import akka.stream.testkit.StreamTestKit
import akka.testkit.TestKit
import hydra.core.ingest.{IngestionParams, RetryStrategy}
import hydra.core.produce.ValidationStrategy

import scala.collection.immutable._

/**
  * Created by alexsilva on 3/17/17.
  */
class HttpRequestFactorySpec extends TestKit(ActorSystem()) with Matchers with FunSpecLike with ScalaFutures {
  describe("When build a HydraRequest from HTTP") {
    it("builds") {
      implicit val mat = ActorMaterializer()
      val json = """{"name":"value"}"""
      val httpRequest = HttpRequest(
        HttpMethods.POST,
        headers = Seq(RawHeader("hydra", "awesome"),
          RawHeader(IngestionParams.HYDRA_VALIDATION_STRATEGY, "relaxed"),
          RawHeader(IngestionParams.HYDRA_RETRY_STRATEGY, "until-success")),
        uri = "/test",
        entity = HttpEntity(MediaTypes.`application/json`, json))
      val req = new HttpRequestFactory().createRequest(Some("label"), httpRequest)
      whenReady(req) { req =>
        req.payload shouldBe json
        req.label shouldBe Some("label")
        req.metadataValue("hydra") shouldBe Some("awesome")
        req.validationStrategy shouldBe ValidationStrategy.Relaxed
        req.retryStrategy shouldBe RetryStrategy.UntilSuccess
      }
    }
  }
}
