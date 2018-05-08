package hydra.core

import com.typesafe.config.ConfigFactory
import hydra.common.auth.NoSecurityAuthenticator
import org.scalatest.{FlatSpecLike, Matchers}

import scala.concurrent.duration._

class SettingsSpec extends Matchers with FlatSpecLike {
  "The Settings " should "have the right ingest topic name in" in {
    Settings.HydraSettings.IngestTopicName shouldBe "hydra-ingest"
  }

  it should "use the refresh interval in the config" in {
    val config = ConfigFactory.parseString(
      """
        |schema.metadata.refresh.interval = 2 minutes
      """.stripMargin)
    new Settings(config).SchemaMetadataRefreshInterval shouldBe 2.minutes
  }
}
