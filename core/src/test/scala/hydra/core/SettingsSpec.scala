package hydra.core

import org.scalatest.{FlatSpecLike, Matchers}

class SettingsSpec extends Matchers with FlatSpecLike {
  "The Settings " should "have the right ingest topic name in" in {
    Settings.IngestTopicName shouldBe "hydra-ingest"
  }
}
