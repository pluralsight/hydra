package hydra.common.util

import hydra.common.testing.DummyActor
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 3/2/17.
  */
class AvroUtilsSpec extends Matchers with FunSpecLike {

  describe("When using AvroUtils") {
    it("replaces invalid characters") {
      AvroUtils.cleanName("!test") shouldBe "_test"
      AvroUtils.cleanName("?test") shouldBe "_test"
      AvroUtils.cleanName("_test") shouldBe "_test"
      AvroUtils.cleanName("test") shouldBe "test"
      AvroUtils.cleanName("1test") shouldBe "1test"
    }
  }

}


