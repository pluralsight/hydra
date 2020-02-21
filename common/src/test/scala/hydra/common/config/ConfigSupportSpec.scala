package hydra.common.config

import java.util.Properties

import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.{FunSpecLike, Matchers}
import ConfigSupport._

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
      val path = Thread
        .currentThread()
        .getContextClassLoader
        .getResource("test.conf")
        .getFile
      loadExternalConfig(
        ConfigFactory
          .parseMap(Map("application.config.location" -> path).asJava)
      ).getString("external-key") shouldBe "external-value"
      loadExternalConfig(ConfigFactory.empty()) shouldBe ConfigFactory.empty()
    }

    it("errors if external config file has a syntax error") {
      val path = Thread
        .currentThread()
        .getContextClassLoader
        .getResource("test-error.conf")
        .getFile
      intercept[ConfigException] {
        loadExternalConfig(
          ConfigFactory
            .parseMap(Map("application.config.location" -> path).asJava)
        )
      }
    }

    it("errors if empty config if path doesn't exists") {
      loadExternalConfig(
        ConfigFactory.parseMap(
          Map(
            "application.config.location"
              -> "this-doesnt-exist"
          ).asJava
        )
      ) shouldBe ConfigFactory.empty()
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
