package hydra.common.alerting

import scala.util.control.NoStackTrace

sealed trait StreamsNotificationsError extends NoStackTrace {
  def message: String

  override def getMessage: String = message
}

object StreamsNotificationsError {
  case class CannotParseNotificationUri(uri: String) extends StreamsNotificationsError {
    override val message: String = s"Cannot parse uri [$uri] for sending notifications."
  }

  case class InvalidUriProvided(uri: String) extends StreamsNotificationsError {
    override val message: String = s"An invalid URI [$uri] was provided. Unable to post notification."
  }
}
