package hydra.core.transport

import org.scalatest.{FlatSpecLike, Matchers}

class AckStrategySpec extends Matchers with FlatSpecLike{

  "the ack strategy companion" should "parse strings" in {
    AckStrategy("transport") shouldBe AckStrategy.TransportAck
    AckStrategy("locAl") shouldBe AckStrategy.LocalAck
    AckStrategy("none") shouldBe AckStrategy.NoAck
    AckStrategy("unknown") shouldBe AckStrategy.NoAck
    AckStrategy(null) shouldBe AckStrategy.NoAck
  }
}
