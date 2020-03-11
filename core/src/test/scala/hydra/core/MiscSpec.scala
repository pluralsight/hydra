package hydra.core

import hydra.core.ingest.HydraRequest
import hydra.core.protocol._
import org.joda.time.DateTime
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.duration._

class MiscSpec extends Matchers with AnyFlatSpecLike {
  "The Hydra Exception " should "be instantiated without a cause" in {
    val ex = new HydraException("error")
    ex.getCause shouldBe null
  }

  "The IngestionTimedOutException" should "have correct values" in {
    val req = HydraRequest("123", "test")
    val ex = new IngestionTimedOut(req, DateTime.now(), 1.second, "test")
    ex.statusCode shouldBe 408
    ex.cause shouldBe an[IngestionTimedOutException]
  }

  "The IngestorUnavailable error" should "have correct values" in {
    val req = HydraRequest("123", "test")
    val err = IngestorUnavailable("test", new IllegalArgumentException, req)
    err.cause shouldBe an[IllegalArgumentException]
    err.statusCode shouldBe 503
  }

  "The InvalidRequest" should "have correct values" in {
    val err = InvalidRequest(new IllegalArgumentException("error"))
    err.cause shouldBe an[IllegalArgumentException]
    err.statusCode.intValue shouldBe 400
    err.completed shouldBe true
    err.message shouldBe "error"

    val err1 = InvalidRequest(new IllegalArgumentException)
    err1.message shouldBe "Unknown error."
  }

  "The InvalidRequestError" should "have correct values" in {
    val req = HydraRequest("123", "test")
    val err = InvalidRequestError(
      "test",
      req,
      DateTime.now,
      new IllegalArgumentException
    )
    err.statusCode shouldBe 400
  }
}
