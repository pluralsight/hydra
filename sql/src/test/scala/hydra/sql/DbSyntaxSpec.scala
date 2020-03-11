package hydra.sql

import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike

/**
  * Created by alexsilva on 7/14/17.
  */
class DbSyntaxSpec extends Matchers with AnyFunSpecLike {

  describe("The Underscore syntax") {
    it("formats property") {
      UnderscoreSyntax.format("TestCase") shouldBe "test_case"
      UnderscoreSyntax.format("test_case") shouldBe "test_case"
      UnderscoreSyntax.format("Test_case") shouldBe "test_case"
      UnderscoreSyntax.format("_test_case") shouldBe "_test_case"
    }
  }

  describe("The NoOp syntax") {
    it("formats property") {
      NoOpSyntax.format("TestCase") shouldBe "TestCase"
    }
  }

}
