package hydra.common.alerting.sender

import eu.timepit.refined.types.string.NonEmptyString
import hydra.common.alerting.AlertProtocol.{NotificationMessage, NotificationScope}
import hydra.common.alerting.{NotificationLevel, NotificationRequestBaker, NotificationType}
import spray.json.JsonFormat

import scala.language.higherKinds

final class InternalNotificationSender[F[_]](internalSource: Option[NonEmptyString], notificationSender: NotificationSender[F]) {

  def send[K: JsonFormat](notificationInfo: NotificationScope,
                          notificationMessage: NotificationMessage[K])
                         (implicit notificationRequestBaker: NotificationRequestBaker[F, Option[NonEmptyString]]): F[Unit] =
    notificationSender.send(notificationInfo, notificationMessage)(internalSource)
}

object InternalNotificationSender {

  final class ScopedInternalNotificationSenderWrapper[A <: NotificationLevel] private[InternalNotificationSender](implicit level: A) {

    def send[F[_], K: JsonFormat](notificationMessage: NotificationMessage[K])
                                 (implicit internalNotificationSender: InternalNotificationSender[F],
                                  notificationRequestBaker: NotificationRequestBaker[F, Option[NonEmptyString]]): F[Unit] =
      internalNotificationSender.send(NotificationScope[A, NotificationType.InternalNotification.type], notificationMessage)
  }

  def apply[A <: NotificationLevel](implicit level: A) = new ScopedInternalNotificationSenderWrapper[A]
}
