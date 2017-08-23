package hydra.core.transport

import org.scalatest.{FlatSpecLike, Matchers}

class DeliveryStrategySpec extends Matchers with FlatSpecLike {

  "the delivery strategy companion" should "parse strings" in {
    DeliveryStrategy("at-least-once") shouldBe DeliveryStrategy.AtLeastOnce
    DeliveryStrategy("best-effort") shouldBe DeliveryStrategy.BestEffort
    DeliveryStrategy("unknown") shouldBe DeliveryStrategy.BestEffort
    DeliveryStrategy(null) shouldBe DeliveryStrategy.BestEffort
  }
}
