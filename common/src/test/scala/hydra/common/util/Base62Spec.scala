package hydra.common.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpecLike

class Base62Spec extends Matchers with AnyFlatSpecLike {

  private val base62 = new Base62()

  "Base62" should "encode number to base62 string" in {
    base62.encode(123456789L) should be("8M0kX")
  }

  it should "decode base62 string to number" in {
    base62.decode("8M0kX") should be(123456789L)
  }

}
