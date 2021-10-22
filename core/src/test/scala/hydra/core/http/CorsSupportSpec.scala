package hydra.core.http

import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.headers.HttpOrigin
import ch.megard.akka.http.cors.scaladsl.model.HttpOriginMatcher
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike

import scala.collection.immutable

class CorsSupportSpec extends Matchers with AnyFunSpecLike with DefaultCorsSupport {

  describe("Default Cors Support") {
    it("is configured for all origins") {
      settings.allowCredentials shouldBe false
      settings.exposedHeaders shouldBe immutable.Seq("Link")
      settings.allowedMethods shouldBe Seq(
        HttpMethods.GET,
        HttpMethods.POST,
        HttpMethods.PUT,
        HttpMethods.DELETE,
        HttpMethods.HEAD,
        HttpMethods.OPTIONS
      )
      settings.allowedOrigins shouldBe HttpOriginMatcher.*
      settings.maxAge shouldBe Some(1800)
      settings.allowGenericHttpRequests shouldBe true
    }
  }

  describe("Cors Support") {
    it("converts * to all origins") {
      val cors = new CorsSupport("*")
      cors.settings.allowedMethods shouldBe Seq(
        HttpMethods.GET,
        HttpMethods.POST,
        HttpMethods.PUT,
        HttpMethods.DELETE,
        HttpMethods.OPTIONS
      )
      cors.settings.allowedOrigins shouldBe HttpOriginMatcher.*
    }

    it("coverts null to all origins") {
      val cors = new CorsSupport(null);
      cors.settings.allowedOrigins shouldBe HttpOriginMatcher.*
    }

    it("converts string param to http origin") {
      val origin = "http://my-awesome-origin.com"
      val cors = new CorsSupport(origin)
      cors.settings.allowedOrigins shouldBe HttpOriginMatcher(HttpOrigin(origin))
    }
  }

}
