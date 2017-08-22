package hydra.core.transport

import org.scalatest.{FlatSpecLike, Matchers}

class RetryStrategySpec extends Matchers with FlatSpecLike {

  "the retry strategy companion" should "parse strings" in {
    RetryStrategy("persist") shouldBe RetryStrategy.Persist
    RetryStrategy("ignore") shouldBe RetryStrategy.Ignore
    RetryStrategy("unknown") shouldBe RetryStrategy.Ignore
    RetryStrategy(null) shouldBe RetryStrategy.Ignore
  }
}
