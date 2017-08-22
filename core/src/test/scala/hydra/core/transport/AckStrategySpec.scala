package hydra.core.transport

import org.scalatest.{FlatSpecLike, Matchers}

class AckStrategySpec extends Matchers with FlatSpecLike{

  "the ack strategy companion" should "parse strings" in {
    AckStrategy("explicit") shouldBe AckStrategy.Explicit
    AckStrategy("none") shouldBe AckStrategy.None
    AckStrategy("unknown") shouldBe AckStrategy.None
    AckStrategy(null) shouldBe AckStrategy.None
  }
}
