package hydra.kafka.util

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, ZoneId}

object GenericUtils {
  def postCutOffDate(givenDate: Option[Instant], cutOffDate: String): Boolean = {
    val cutOffDateInstant = LocalDate.parse(cutOffDate, DateTimeFormatter.BASIC_ISO_DATE)
      .atStartOfDay(ZoneId.of("UTC+05:30")).toInstant //TODO: Consult on the time-zone to use

    givenDate.exists(_.toEpochMilli >= cutOffDateInstant.toEpochMilli)
  }
}
