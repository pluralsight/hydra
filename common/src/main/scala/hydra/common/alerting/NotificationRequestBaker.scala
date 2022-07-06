package hydra.common.alerting

import cats.Monad
import cats.syntax.all._
import eu.timepit.refined.types.string.NonEmptyString
import hydra.common.alerting.AlertProtocol._
import spray.json.JsonFormat

import java.net.InetAddress
import scala.language.higherKinds

trait NotificationRequestBaker[F[_], T] {

  def bake[K: JsonFormat](notificationInfo: NotificationScope,
                          notificationMessage: NotificationMessage[K])
                         (source: T): F[Option[NotificationRequest]]
}

object NotificationRequestBaker {

  implicit def basicNotificationBacker[F[_]: Monad]: NotificationRequestBaker[F, Option[NonEmptyString]] =
    new NotificationRequestBaker[F, Option[NonEmptyString]] {
      override def bake[T: JsonFormat](notificationInfo: NotificationScope,
                                       notificationMessage: NotificationMessage[T])
                                      (notificationUrl: Option[NonEmptyString]): F[Option[NotificationRequest]] = {
        NotificationRequest(
          notificationInfo,
          StreamsNotification.make(notificationMessage, notificationInfo, DefaultNotificationProperties),
          notificationUrl
        ).some.pure
      }
    }


  lazy val DefaultNotificationProperties: Map[String, String] =
    Map(
      "host" -> InetAddress.getLocalHost.getHostName
    )

}
