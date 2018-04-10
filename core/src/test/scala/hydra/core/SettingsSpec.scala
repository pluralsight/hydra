package hydra.core

import com.typesafe.config.ConfigFactory
import hydra.core.auth.NoSecurityAuthenticator
import org.scalatest.{FlatSpecLike, Matchers}

class SettingsSpec extends Matchers with FlatSpecLike {
  "The Settings " should "have the right ingest topic name in" in {
    Settings.HydraSettings.IngestTopicName shouldBe "hydra-ingest"
  }

  it should "instantiate the configured authenticator" in {
    val config = ConfigFactory.parseString(
      """
        |hydra.http.authenticator = hydra.core.auth.NoSecurityAuthenticator
      """.stripMargin)
    new Settings(config).Authenticator shouldBe a[NoSecurityAuthenticator]
  }
}
