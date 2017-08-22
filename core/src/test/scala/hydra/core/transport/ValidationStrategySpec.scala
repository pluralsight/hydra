package hydra.core.transport

import org.scalatest.{FlatSpecLike, Matchers}

class ValidationStrategySpec extends Matchers with FlatSpecLike{

  "the validation strategy companion" should "parse strings" in {
    ValidationStrategy("relaxed") shouldBe ValidationStrategy.Relaxed
    ValidationStrategy("strict") shouldBe ValidationStrategy.Strict
    ValidationStrategy("unknown") shouldBe ValidationStrategy.Strict
    ValidationStrategy(null) shouldBe ValidationStrategy.Strict
  }
}
