package hydra.avro.util

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 7/6/17.
  */
class ConfigUtilsSpec extends Matchers with FunSpecLike {

  describe("When using ConfigUtils") {
    it("converts to properties") {
      val cfg = ConfigFactory.parseString(
        """
          |test.name = test
          |test.class=test-class
        """.stripMargin)

      val props = new Properties
      props.put("test.name", "test")
      props.put("test.class", "test-class")

      ConfigUtils.toProperties(cfg) shouldBe props
    }
  }
}
