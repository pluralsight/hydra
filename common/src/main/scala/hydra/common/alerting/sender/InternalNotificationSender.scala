package hydra.common.alerting.sender

import cats.implicits.catsSyntaxOptionId
import eu.timepit.refined.types.string.NonEmptyString
import hydra.common.alerting.AlertProtocol.{NotificationMessage, NotificationScope}
import hydra.common.alerting.{NotificationLevel, NotificationRequestBuilder, NotificationType}
import spray.json.JsonWriter

import scala.language.higherKinds

final class InternalNotificationSender[F[_]](internalSource: Option[NonEmptyString], notificationSender: NotificationSender[F]) {

  def send[K: JsonWriter](notificationInfo: NotificationScope,
                          notificationMessage: NotificationMessage[K])
                         (implicit notificationRequestBuilder: NotificationRequestBuilder[F, Option[NonEmptyString]]): F[Unit] =
    notificationSender.send(notificationInfo, notificationMessage, internalSource)
}

object InternalNotificationSender {

  final class ScopedInternalNotificationSenderWrapper[A <: NotificationLevel] private[InternalNotificationSender](implicit level: A) {

    def send[F[_], K: JsonWriter](notificationMessage: NotificationMessage[K])
                                 (implicit internalNotificationSender: InternalNotificationSender[F],
                                  notificationRequestBaker: NotificationRequestBuilder[F, Option[NonEmptyString]]): F[Unit] =
      internalNotificationSender.send(NotificationScope(level, NotificationType.InternalNotification.some), notificationMessage)
  }

  def apply[A <: NotificationLevel](implicit level: A) = new ScopedInternalNotificationSenderWrapper[A]
}
