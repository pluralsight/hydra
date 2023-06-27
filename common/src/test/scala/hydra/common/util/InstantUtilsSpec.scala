package hydra.common.util

import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class InstantUtilsSpec extends Matchers with AnyFlatSpecLike {

  "InstantUtils" should "convert a date string YYYYMMDD to an instant object" in {
    InstantUtils.dateStringToInstant("20230627") shouldBe Instant.parse("2023-06-27T00:00:00Z")
  }
}
