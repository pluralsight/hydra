package hydra.common.alerting

import cats.Monad
import cats.syntax.all._
import eu.timepit.refined.types.string.NonEmptyString
import hydra.common.alerting.AlertProtocol._
import spray.json.JsonWriter

import java.net.InetAddress
import scala.language.higherKinds

trait NotificationRequestBuilder[F[_], T] {

  def build[K: JsonWriter](notificationInfo: NotificationScope,
                           notificationMessage: NotificationMessage[K],
                           source: T): F[Option[NotificationRequest]]
}

object NotificationRequestBuilder {

  implicit def basicNotificationBuilder[F[_]: Monad]: NotificationRequestBuilder[F, Option[NonEmptyString]] =
    new NotificationRequestBuilder[F, Option[NonEmptyString]] {
      override def build[T: JsonWriter](notificationInfo: NotificationScope,
                                        notificationMessage: NotificationMessage[T],
                                        notificationUrl: Option[NonEmptyString]): F[Option[NotificationRequest]] =
        NotificationRequest(
          notificationInfo,
          StreamsNotification.apply(notificationMessage, notificationInfo, DefaultNotificationProperties),
          notificationUrl
        ).some.pure
    }

  lazy val DefaultNotificationProperties: Map[String, String] =
    Map(
      "host" -> InetAddress.getLocalHost.getHostName
    )

}
