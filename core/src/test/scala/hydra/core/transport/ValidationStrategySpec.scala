package hydra.core.transport

import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpecLike

class ValidationStrategySpec extends Matchers with AnyFlatSpecLike {

  "the validation strategy companion" should "parse strings" in {
    ValidationStrategy("relaxed") shouldBe ValidationStrategy.Relaxed
    ValidationStrategy("strict") shouldBe ValidationStrategy.Strict
    ValidationStrategy("unknown") shouldBe ValidationStrategy.Strict
    ValidationStrategy(null) shouldBe ValidationStrategy.Strict
  }
}
