package hydra.core.ingest

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import akka.testkit.TestKit
import hydra.core.protocol.InitiateRequest
import hydra.core.transport.{AckStrategy, ValidationStrategy}
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.duration._

/**
  * Created by alexsilva on 3/22/17.
  */
class HydraRequestSpec
    extends TestKit(ActorSystem("hydra"))
    with Matchers
    with AnyFunSpecLike
    with BeforeAndAfterAll {

  override def afterAll = TestKit.shutdownActorSystem(system)

  describe("A HydraRequest") {
    it("return metadata value regardless of case") {
      val hr =
        HydraRequest("123", metadata = Map("test" -> "value"), payload = "test")
      hr.metadataValue("TEST").get shouldBe "value"
      hr.metadataValue("test").get shouldBe "value"
      hr.metadataValue("TeSt").get shouldBe "value"
      hr.metadataValue("TEST1") shouldBe None
    }

    it("compares metadata values regardless of case") {
      val hr =
        HydraRequest("123", metadata = Map("test" -> "value"), payload = "test")
      hr.metadataValueEquals("TEST", "value") shouldBe true
      hr.metadataValueEquals("TEST1", "value") shouldBe false
      hr.metadataValueEquals("TEST", "?") shouldBe false
    }

    it("checks if metadata value exists regardless of case") {
      val hr =
        HydraRequest("123", metadata = Map("test" -> "value"), payload = "test")
      hr.hasMetadata("test") shouldBe true
      hr.hasMetadata("TEST") shouldBe true
      hr.hasMetadata("TEST1") shouldBe false

    }

    it("copies correlation id") {
      val hr =
        HydraRequest("123", metadata = Map("test" -> "value"), payload = "test")
          .withCorrelationId("24")
      hr.correlationId shouldBe "24"
      hr.payload shouldBe "test"
    }

    it("copies validation strategy") {
      val hr =
        HydraRequest("123", metadata = Map("test" -> "value"), payload = "test")
          .withValidationStratetegy(ValidationStrategy.Relaxed)
      hr.correlationId shouldBe "123"
      hr.payload shouldBe "test"
      hr.validationStrategy shouldBe ValidationStrategy.Relaxed
    }

    it("copies request strategy") {
      val hr =
        HydraRequest("123", metadata = Map("test" -> "value"), payload = "test")
          .withAckStrategy(AckStrategy.Replicated)
      hr.correlationId shouldBe "123"
      hr.payload shouldBe "test"
      hr.ackStrategy shouldBe AckStrategy.Replicated
    }

    it("copies metadata") {
      val hr =
        HydraRequest("123", metadata = Map("test" -> "value"), payload = "test")
      hr.withMetadata("new" -> "value").metadata shouldBe Map(
        "test" -> "value",
        "new" -> "value"
      )
      hr.withMetadata("test" -> "newvalue").metadata shouldBe Map(
        "test" -> "newvalue"
      )
    }

  }
}
