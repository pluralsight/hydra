package hydra.common.util

import org.joda.time.{DateTime, DateTimeZone}

object TimeUtils {

  def now(): String = DateTime.now(DateTimeZone.UTC).toString("yyyy-MM-dd HH:mm:ss")

}
