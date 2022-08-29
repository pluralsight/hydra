package hydra.common.alerting

sealed trait NotificationType extends Product with Serializable

object NotificationType {
  trait StreamingNotificationType extends NotificationType

  case object InternalNotification extends NotificationType
}
