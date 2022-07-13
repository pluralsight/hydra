package hydra.common.util

import akka.http.scaladsl.model.Uri
import hydra.common.alerting.NotificationsError

import scala.util.{Failure, Try}

object UriUtils {

  def convertUrl(uriStr: String): Either[Throwable, Uri] =
    Try(Uri(uriStr))
      .recoverWith { case _ => Failure(NotificationsError.CannotParseNotificationUri(uriStr)) }
      .toEither

}
