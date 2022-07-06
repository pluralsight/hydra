package hydra.common

import akka.http.scaladsl.model.Uri

import scala.util.{Failure, Try}

package object alerting {

  def convertUrl(uriStr: String): Either[Throwable, Uri] =
    Try(Uri(uriStr))
      .recoverWith { case _ => Failure(StreamsNotificationsError.CannotParseNotificationUri(uriStr)) }
      .toEither

}
