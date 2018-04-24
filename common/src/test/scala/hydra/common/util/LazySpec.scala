package hydra.common.util

import org.scalatest.{FlatSpecLike, Matchers}

class LazySpec extends Matchers with FlatSpecLike {

  import Lazy._

  "The Lazy object" should "not initialize on construction" in {
    val z = lazily{"a"}
    z.isEvaluated shouldBe false
  }

  it should "initialize on method call" in {
    val z = lazily{"a"}
    z()
    z.isEvaluated shouldBe true
  }

}
