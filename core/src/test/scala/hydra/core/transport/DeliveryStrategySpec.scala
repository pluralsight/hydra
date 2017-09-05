package hydra.core.transport

import org.scalatest.{FlatSpecLike, Matchers}

class DeliveryStrategySpec extends Matchers with FlatSpecLike {

  "the delivery strategy companion" should "parse strings" in {
    DeliveryStrategy("at-least-once") shouldBe DeliveryStrategy.AtLeastOnce
    DeliveryStrategy("best-effort") shouldBe DeliveryStrategy.AtMostOnce
    DeliveryStrategy("unknown") shouldBe DeliveryStrategy.AtMostOnce
    DeliveryStrategy(null) shouldBe DeliveryStrategy.AtMostOnce
  }
}
