package hydra.common

import akka.http.scaladsl.model.Uri
import spray.json.{JsString, JsValue, JsonFormat, JsonWriter, deserializationError}

import scala.util.{Failure, Try}

package object alerting {

  def convertUrl(uriStr: String): Either[Throwable, Uri] =
    Try(Uri(uriStr))
      .recoverWith { case _ => Failure(NotificationsError.CannotParseNotificationUri(uriStr)) }
      .toEither

}
