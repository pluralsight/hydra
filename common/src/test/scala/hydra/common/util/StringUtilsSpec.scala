package hydra.common.util

import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 3/2/17.
  */
class StringUtilsSpec extends Matchers with FunSpecLike {

  describe("When using StringUtils") {
    it("converts camel case to underscore case") {
      StringUtils.camel2underscores("TestName") shouldBe "test_name"
      StringUtils.camel2underscores("_TestName") shouldBe "_test_name"
      StringUtils.camel2underscores("Test_NameText") shouldBe "test__name_text"
    }
    it("converts underscore case to camel case") {

      StringUtils.underscores2camel("test__name_text") shouldBe "Test_NameText"

    }
    it("doesn't allow underscores at the end when converting to camel case") {
      intercept[IllegalArgumentException] {
        StringUtils.underscores2camel("Test_NameText_")
      }
    }
  }

}
