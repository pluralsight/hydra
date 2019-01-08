package hydra.core.persistence

import java.sql.Timestamp

import org.joda.time.DateTime
import slick.jdbc.JdbcProfile

trait ProfileComponent {
  val profile: JdbcProfile

  import profile.api._

  implicit lazy val dateTimeMapper = MappedColumnType.base[DateTime, Timestamp](
    e => new Timestamp(e.getMillis), s => new DateTime(s.getTime))

  // Convert from joda DateTime to java.sql.Timestamp
  def jodaToSql(dateTime: DateTime): Timestamp = new Timestamp(dateTime.getMillis)

  // Convert from java.sql.Timestamp to joda DateTime
  def sqlToJoda(timestamp: Timestamp): DateTime = new DateTime(timestamp.getTime)

}