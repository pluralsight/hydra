package hydra.common.util

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, ZoneId}

object InstantUtils {

  def dateStringToInstant(date: String): Instant =
    LocalDate
      .parse(date, DateTimeFormatter.BASIC_ISO_DATE)
      .atStartOfDay(ZoneId.of("UTC"))
      .toInstant

}
