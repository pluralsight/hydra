package hydra.common.alerting.sender

import cats.effect.Sync
import cats.syntax.all._
import hydra.common.alerting.AlertProtocol.{NotificationMessage, NotificationRequest, NotificationScope}
import hydra.common.alerting._
import hydra.common.util.UriUtils.convertUrl
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import spray.json.JsonWriter

import scala.language.higherKinds

trait NotificationSender[F[_]] {

  def send[T, K: JsonWriter](notificationInfo: NotificationScope, notificationMessage: NotificationMessage[K], source: T)
                            (implicit notificationRequestBaker: NotificationRequestBuilder[F, T]): F[Unit]

}

object NotificationSender {

  class BasicNotificationSender[F[_] : Sync](client: NotificationsClient[F], logger: Logger[F]) extends NotificationSender[F] {

    def send[T, K: JsonWriter](notificationInfo: NotificationScope, notificationMessage: NotificationMessage[K], source: T)
                              (implicit notificationRequestBaker: NotificationRequestBuilder[F, T]): F[Unit] = {

      notificationRequestBaker.build(notificationInfo, notificationMessage, source)
        .flatMap {
          case Some(request) =>
            sendNotification(request)
          case None => Sync[F].unit
        }.handleErrorWith(error => logger.warn(s"Can not send notification [$notificationMessage]. Reason: $error"))
    }

    protected def sendNotification(notificationRequest: NotificationRequest): F[Unit] = {
      notificationRequest.url
        .map(uriStr => convertUrl(uriStr.value))
        .fold(Sync[F].unit)(Sync[F].fromEither(_)
          .flatMap {
            client.postNotification(
              _,
              notificationRequest.streamsNotification
            ).void
          }
        )
    }

  }

  def apply[F[_] : Sync](client: NotificationsClient[F]): F[NotificationSender[F]] = {
    Slf4jLogger.fromClass(getClass).map { logger => new BasicNotificationSender[F](client, logger) }
  }

  def apply[A <: NotificationLevel, B <: NotificationType](implicit level: A, notificationType: B): ScopedNotificationSenderWrapper[A, B] =
    new ScopedNotificationSenderWrapper[A, B]()

  final class ScopedNotificationSenderWrapper[A <: NotificationLevel, B <: NotificationType] private[NotificationSender](implicit level: A, notificationType: B) {

    def send[F[_], T, K: JsonWriter](notificationMessage: NotificationMessage[K],
                                     source: T
                                    )(implicit notificationSender: NotificationSender[F],
                                      notificationRequestBaker: NotificationRequestBuilder[F, T]
                                    ): F[Unit] =
      notificationSender.send(NotificationScope(level, Option(notificationType)), notificationMessage, source)

  }

}

