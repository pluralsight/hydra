package hydra.ingest.app

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import cats.syntax.all._

import java.time.Instant

final class AppConfigSpec extends AnyFlatSpec with Matchers {

  it should "parse a list of comma separated strings into a set" in {
    AppConfig.decodeSetStrings.decode(None, "test,test2,test3") shouldBe Set("test", "test2", "test3").asRight
  }

  it should "parse an empty string into an empty set" in {
    AppConfig.decodeSetStrings.decode(None, "") shouldBe Set.empty.asRight
  }

  it should "parse a string date to an instant" in {
    AppConfig.dateStringToInstantDecoder.decode(None, "20230705") shouldBe Instant.parse("2023-07-05T00:00:00Z").asRight
  }
}
