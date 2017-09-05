package hydra.core

import org.scalatest.{FlatSpecLike, Matchers}

class MiscSpec extends Matchers with FlatSpecLike {
  "The Hydra Exception " should "be instantiated without a cause" in {
    val ex = new HydraException("error")
    ex.getCause shouldBe null
  }
}
