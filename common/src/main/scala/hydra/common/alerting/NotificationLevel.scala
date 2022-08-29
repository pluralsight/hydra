package hydra.common.alerting

sealed trait NotificationLevel extends Product with Serializable

object NotificationLevel {
  case object Debug extends NotificationLevel
  case object Info  extends NotificationLevel
  case object Warn  extends NotificationLevel
  case object Error extends NotificationLevel

  def getAllowedLevels(level: NotificationLevel): Vector[NotificationLevel] = {
    level match {
      case Debug => Vector(Debug, Info, Warn, Error)
      case Info  => Vector(Info, Warn, Error)
      case Warn  => Vector(Warn, Error)
      case Error => Vector(Error)
      case _     => Vector.empty
    }
  }

  val values = Set(Debug, Info, Warn, Error)

  def from(value: String): Option[NotificationLevel] = values.find(_.toString.toLowerCase == value.toLowerCase)

}
