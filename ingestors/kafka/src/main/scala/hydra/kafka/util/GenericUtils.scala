package hydra.kafka.util

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, ZoneId}

object GenericUtils {
  def postCutOffDate(givenDate: Option[Instant], cutOffDate: String): Boolean =
    givenDate.exists(
      _.toEpochMilli > dateStringToInstant(cutOffDate).toEpochMilli
    )

  def dateStringToInstant(date: String): Instant =
    LocalDate
      .parse(date, DateTimeFormatter.BASIC_ISO_DATE)
      .atStartOfDay(ZoneId.of("UTC"))
      .toInstant
}
