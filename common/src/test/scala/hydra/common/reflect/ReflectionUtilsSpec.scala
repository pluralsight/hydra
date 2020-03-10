package hydra.common.reflect

import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike

/**
  * Created by alexsilva on 3/3/17.
  */
class ReflectionUtilsSpec extends Matchers with AnyFunSpecLike {
  describe("When using ReflectionUtils") {
    it("Instantiates a class with constructor params") {
      ReflectionUtils.instantiateType[TestClass](List("value")) shouldBe TestClass(
        "value"
      )
      ReflectionUtils.instantiateClass(classOf[TestClass], List("value")) shouldBe TestClass(
        "value"
      )
    }

    it("Identifies companion objects") {
      ReflectionUtils.companionOf[TestClass] shouldBe TestClass
      ReflectionUtils.companionOf(classOf[TestClass]) shouldBe TestClass
    }

    it("Instantiates classes by name") {
      val obj = ReflectionUtils.instantiateClassByName[TestClass](
        "hydra.common.reflect.TestClass",
        List("value")
      )
      obj shouldBe TestClass("value")
    }

    it("Instantiates classes with empty constructor lists") {
      val obj =
        ReflectionUtils.instantiateClass(classOf[TestDefaultConstructor])
      obj shouldBe a[TestDefaultConstructor]
    }

  }

}

class TestDefaultConstructor

case class TestClass(value: String)

object TestClass {
  def apply(n: Int) = new TestClass(n.toString)
}
