package hydra.common.reflect

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpecLike

/**
  * Created by alexsilva on 3/3/17.
  */
class ComponentInstantiatorSpec extends Matchers with AnyFlatSpecLike {

  "The ComponentInstantiator" should "instantiate a class with no args" in {
    val c = ComponentInstantiator.instantiate(
      classOf[TestInstanceNoObj],
      List(ConfigFactory.empty())
    )
    c.get shouldBe a[TestInstanceNoObj]
  }

  it should "instantiate a class using its companion object apply method" in {
    val c = ComponentInstantiator.instantiate(
      classOf[TestInstance],
      List(ConfigFactory.parseString("value=1"))
    )
    c.get shouldBe a[TestInstance]
    c.get.value shouldBe 1
  }

  it should "instantiate a class using a supplied companion object method" in {
    val c = ComponentInstantiator.instantiate(
      classOf[TestInstance],
      List(2: java.lang.Integer),
      "build"
    )
    c.get.value shouldBe 2
  }

  it should "error if no method name found" in {
    val c = ComponentInstantiator.instantiate(
      classOf[TestInstance],
      List(2: java.lang.Integer),
      "unknown"
    )
    c.isFailure shouldBe true
  }

}

class TestInstanceNoObj

case class TestInstance(value: Int)

object TestInstance {
  def apply(config: Config) = new TestInstance(config.getInt("value"))

  def build(value: java.lang.Integer) = new TestInstance(value)
}
