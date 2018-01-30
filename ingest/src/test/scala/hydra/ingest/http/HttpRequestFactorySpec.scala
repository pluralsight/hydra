package hydra.ingest.http

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import hydra.core.ingest.RequestParams
import hydra.core.transport.{AckStrategy, ValidationStrategy}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.collection.immutable._
import scala.concurrent.duration._

/**
  * Created by alexsilva on 3/17/17.
  */
class HttpRequestFactorySpec extends TestKit(ActorSystem()) with Matchers with FunSpecLike
  with ScalaFutures with BeforeAndAfterAll {

  override def afterAll = TestKit.shutdownActorSystem(system, verifySystemShutdown = true,duration=10 seconds)

  describe("When build a HydraRequest from HTTP") {
    it("builds") {
      implicit val mat = ActorMaterializer()
      val json = """{"name":"value"}"""
      val httpRequest = HttpRequest(
        HttpMethods.POST,
        headers = Seq(RawHeader("hydra", "awesome"),
          RawHeader(RequestParams.HYDRA_VALIDATION_STRATEGY, "relaxed"),
          RawHeader(RequestParams.HYDRA_ACK_STRATEGY, "replicated")),
        uri = "/test",
        entity = HttpEntity(MediaTypes.`application/json`, json))
      val req = new HttpRequestFactory().createRequest("123", httpRequest)
      whenReady(req) { req =>
        req.payload shouldBe json
        req.correlationId shouldBe "123"
        req.metadataValue("hydra") shouldBe Some("awesome")
        req.validationStrategy shouldBe ValidationStrategy.Relaxed
        req.ackStrategy shouldBe AckStrategy.Replicated
      }
    }

    it("builds with default strategy values") {
      implicit val mat = ActorMaterializer()
      val json = """{"name":"value"}"""
      val httpRequest = HttpRequest(
        HttpMethods.POST,
        headers = Seq(RawHeader("hydra", "awesome")),
        uri = "/test",
        entity = HttpEntity(MediaTypes.`application/json`, json))
      val req = new HttpRequestFactory().createRequest("123", httpRequest)
      whenReady(req) { req =>
        req.payload shouldBe json
        req.correlationId shouldBe "123"
        req.metadataValue("hydra") shouldBe Some("awesome")
        req.validationStrategy shouldBe ValidationStrategy.Strict
        req.ackStrategy shouldBe AckStrategy.NoAck
      }
    }
  }
}
