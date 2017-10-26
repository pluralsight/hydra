package hydra.sql

import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 7/14/17.
  */
class DbSyntaxSpec extends Matchers with FunSpecLike {

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
