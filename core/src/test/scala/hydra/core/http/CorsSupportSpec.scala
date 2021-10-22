package hydra.core.http

import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.headers.HttpOrigin
import ch.megard.akka.http.cors.scaladsl.model.HttpOriginMatcher
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike

import scala.collection.immutable

class CorsSupportSpec extends Matchers with AnyFunSpecLike with DefaultCorsSupport {

  describe("Cors Support") {
    it("is configured for vnerd") {
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
      settings.allowedOrigins shouldBe HttpOriginMatcher(HttpOrigin("https://*.vnerd.com"))
      settings.maxAge shouldBe Some(1800)
      settings.allowGenericHttpRequests shouldBe true
    }
  }

}
