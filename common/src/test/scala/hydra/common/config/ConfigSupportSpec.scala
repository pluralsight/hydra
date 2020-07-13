package hydra.common.config

import java.util.Properties

import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike
import ConfigSupport._

import scala.collection.JavaConverters._

/**
  * Created by alexsilva on 3/2/17.
  */
class ConfigSupportSpec extends Matchers with AnyFunSpecLike with ConfigSupport {

  describe("When configuring") {
    it("has the correct application name") {
      applicationName shouldBe "hydraTest"
    }

    it("converts a config to map") {
      val map = Map(
        "test-key" -> "test-value",
        "test-number" -> 1,
        "test.boolean" -> false
      )

      ConfigFactory.parseMap(map.asJava).toMap shouldBe map

    }

    it("converts a config object to map") {
      val map = Map(
        "test-key" -> "test-value",
        "test-number" -> 1,
        "test.boolean" -> false
      )

      ConfigSupport.toMap(ConfigFactory.parseMap(map.asJava).root()) shouldBe map

    }

    it("converts a map to a java properties") {
      val map = Map[String, AnyRef](
        "test-key" -> "test-value",
        "test-number" -> "1",
        "test.boolean" -> "false"
      )
      (map: Properties).getProperty("test.boolean") shouldBe "false"
    }

  }

}
