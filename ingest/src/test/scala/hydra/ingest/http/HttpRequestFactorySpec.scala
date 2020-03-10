package hydra.ingest.http

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import hydra.core.ingest.RequestParams
import hydra.core.transport.{AckStrategy, ValidationStrategy}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.BeforeAndAfterAll

import scala.collection.immutable._
import scala.concurrent.duration._

/**
  * Created by alexsilva on 3/17/17.
  */
class HttpRequestFactorySpec
    extends TestKit(ActorSystem())
    with Matchers
    with AnyFunSpecLike
    with ScalaFutures
    with BeforeAndAfterAll {

  override def afterAll =
    TestKit.shutdownActorSystem(
      system,
      verifySystemShutdown = true,
      duration = 10 seconds
    )

  describe("When build a HydraRequest from HTTP") {
    it("builds") {
      val json = """{"name":"value"}"""
      val httpRequest = HttpRequest(
        HttpMethods.POST,
        headers = Seq(
          RawHeader("hydra", "awesome"),
          RawHeader(RequestParams.HYDRA_VALIDATION_STRATEGY, "relaxed"),
          RawHeader(RequestParams.HydraClientId, "test-client"),
          RawHeader(RequestParams.HYDRA_ACK_STRATEGY, "replicated")
        ),
        uri = "/test",
        entity = HttpEntity(MediaTypes.`application/json`, json)
      )
      val req = new HttpRequestFactory().createRequest("123", httpRequest)
      whenReady(req) { req =>
        req.payload shouldBe json
        req.correlationId shouldBe "123"
        req.metadataValue("hydra") shouldBe Some("awesome")
        req.clientId shouldBe Some("test-client")
        req.validationStrategy shouldBe ValidationStrategy.Relaxed
        req.ackStrategy shouldBe AckStrategy.Replicated
      }
    }

    it("errors out if an ack strategy that doesn't exist is specified") {
      val httpRequest = HttpRequest(
        HttpMethods.POST,
        uri = "/test",
        entity = HttpEntity.Empty,
        headers = Seq(RawHeader(RequestParams.HYDRA_ACK_STRATEGY, "invalid"))
      )
      val req = new HttpRequestFactory().createRequest("123", httpRequest)
      whenReady(req.failed)(_ shouldBe an[IllegalArgumentException])
    }

    it("builds a DELETE request") {
      val httpRequest = HttpRequest(
        HttpMethods.DELETE,
        uri = "/test",
        entity = HttpEntity.Empty
      )
      val req = new HttpRequestFactory().createRequest("123", httpRequest)
      whenReady(req) { req => req.payload shouldBe null }
    }

    it("builds a DELETE request and nulls out payload if it exists") {
      val json = """{"name":"value"}"""
      val httpRequest = HttpRequest(
        HttpMethods.DELETE,
        uri = "/test",
        entity = HttpEntity(MediaTypes.`application/json`, json)
      )
      val req = new HttpRequestFactory().createRequest("123", httpRequest)
      whenReady(req) { req => req.payload shouldBe null }
    }

    it("does not modify empty payloads for non-DELETE requests") {
      val httpRequest =
        HttpRequest(HttpMethods.POST, uri = "/test", entity = HttpEntity.Empty)
      val req = new HttpRequestFactory().createRequest("123", httpRequest)
      whenReady(req) { req => req.payload shouldBe "" }
    }

    it("builds with default strategy values") {
      val json = """{"name":"value"}"""
      val httpRequest = HttpRequest(
        HttpMethods.POST,
        headers = Seq(RawHeader("hydra", "awesome")),
        uri = "/test",
        entity = HttpEntity(MediaTypes.`application/json`, json)
      )
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
