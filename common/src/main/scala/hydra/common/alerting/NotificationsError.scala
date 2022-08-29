package hydra.common.alerting

import scala.util.control.NoStackTrace

sealed trait NotificationsError extends NoStackTrace {
  def message: String

  override def getMessage: String = message
}

object NotificationsError {
  case class CannotParseNotificationUri(uri: String) extends NotificationsError {
    override val message: String = s"Cannot parse uri [$uri] for sending notifications."
  }

  case class InvalidUriProvided(uri: String) extends NotificationsError {
    override val message: String = s"An invalid URI [$uri] was provided. Unable to post notification."
  }
}
