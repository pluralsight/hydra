package hydra.core.http

import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.headers.HttpOriginRange
import ch.megard.akka.http.cors.scaladsl.model.HttpOriginMatcher
import org.scalatest.{FunSpecLike, Matchers}

import scala.collection.immutable

class CorsSupportSpec extends Matchers with FunSpecLike with CorsSupport {

  describe("Cors Support") {
    it ("has sensible defaults") {
      settings.allowCredentials shouldBe false
      settings.exposedHeaders shouldBe immutable.Seq("Link")
      settings.allowedMethods shouldBe Seq(HttpMethods.GET, HttpMethods.POST, HttpMethods.HEAD, HttpMethods.OPTIONS)
      settings.allowedOrigins shouldBe HttpOriginMatcher.*
      settings.maxAge shouldBe Some(1800)
      settings.allowGenericHttpRequests shouldBe true
    }
  }

}
