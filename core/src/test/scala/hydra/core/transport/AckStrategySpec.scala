package hydra.core.transport

import hydra.core.transport.AckStrategy.NoAck
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpecLike

class AckStrategySpec extends Matchers with AnyFlatSpecLike {

  "the ack strategy companion" should "parse strings" in {
    AckStrategy("replicated").get shouldBe AckStrategy.Replicated
    AckStrategy("persIsted").get shouldBe AckStrategy.Persisted
    intercept[IllegalArgumentException] {
      AckStrategy("none").get
    }
    intercept[IllegalArgumentException] {
      AckStrategy("unknown").get
    }

    //the "default" options, if no Ack is specified
    AckStrategy(null).get shouldBe NoAck
    AckStrategy("").get shouldBe NoAck
  }
}
