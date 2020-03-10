package hydra.common.util

import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike

/**
  * Created by alexsilva on 3/2/17.
  */
class DateUtilsSpec extends Matchers with AnyFunSpecLike {

  private val iso8601format = ISODateTimeFormat.dateTimeNoMillis()
  private val iso8601withMillis = ISODateTimeFormat.dateTime()

  import DateUtils._

  val now = DateTime.now()

  val f = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  val dt = f.parseDateTime("2017-01-10 23:13:26")

  describe("When using DateUtils") {
    it("converts to ISO format") {
      iso8601(now, false) shouldBe iso8601format.print(now)
      iso8601(now, true) shouldBe iso8601withMillis.print(now)
    }

    it("converts to UTC") {
      val dt = new DateTime(1234567000, DateTimeZone.UTC)
      dtFromUtcSeconds(1234567) shouldBe dt
      dtFromIso8601("1970-01-15T06:56:07Z") shouldBe dt
    }

    it("implicitly converts to the wrapper") {
      val dtw: DateTimeWrapper = dt
      dtw shouldBe DateTimeWrapper(dt)
    }

    it("sorts and compares dates") {
      val dtw: DateTimeWrapper = dt
      dtw.compare(now) should be < 0

      dtw.compare(now, dt) should be > 0

      Seq(now, dt).sorted shouldBe Seq(dt, now)
    }
  }

}
