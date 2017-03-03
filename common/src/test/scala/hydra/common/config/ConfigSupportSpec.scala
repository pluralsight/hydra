package hydra.common.config

import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpecLike, Matchers}

import scala.collection.JavaConverters._

/**
  * Created by alexsilva on 3/2/17.
  */
class ConfigSupportSpec extends Matchers with FunSpecLike with ConfigSupport {

  describe("When configuring") {
    it("has the correct application name") {
      applicationName shouldBe "hydraTest"
    }

    it("loads external files properly") {
      val path = Thread.currentThread().getContextClassLoader.getResource("test.conf").getFile
      loadExternalConfig(ConfigFactory.parseMap(Map("application.config.location" -> path).asJava))
        .getString("external-key") shouldBe "external-value"
      loadExternalConfig(ConfigFactory.empty()) shouldBe ConfigFactory.empty()
    }

    it("converts a config to map") {
      val map = Map(
        "test-key" -> "test-value",
        "test-number" -> 1,
        "test.boolean" -> false
      )

      toMap(ConfigFactory.parseMap(map.asJava)) shouldBe map

    }

    it("converts a config object to map") {
      val map = Map(
        "test-key" -> "test-value",
        "test-number" -> 1,
        "test.boolean" -> false
      )

      toMap(ConfigFactory.parseMap(map.asJava).root()) shouldBe map

    }

  }

}
