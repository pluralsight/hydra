package hydra.common.alerting

import akka.http.scaladsl.model.{HttpMethods, HttpRequest, IllegalUriException, Uri}
import akka.stream.Materializer
import cats.effect.{Async, ContextShift}
import cats.syntax.all._
import hydra.common.alerting.AlertProtocol.StreamsNotification
import hydra.common.http.HttpRequestor
import hydra.common.logging.LoggingAdapter
import spray.json.{JsValue, _}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.language.higherKinds

trait NotificationsClient[F[_]] extends LoggingAdapter {
  def postNotification(uri: Uri, notification: StreamsNotification): F[JsValue]
}

object NotificationsClient {
  def make[F[_] : ContextShift](httpRequestor: HttpRequestor)(implicit eff: Async[F], m: Materializer, ex: ExecutionContext): F[NotificationsClient[F]] = {
    eff.delay {
      new NotificationsClient[F] {
        override def postNotification(uri: Uri, notification: StreamsNotification): F[JsValue] = {
          val request = HttpRequest(HttpMethods.POST, uri, entity = notification.toJson.prettyPrint)

          val requestFuture = httpRequestor
            .makeRequest(request)
            .flatMap { resp =>
              resp
                .entity
                .toStrict(500.millis)
                .map(_.data.utf8String)
                .map(_.parseJson)
            }

          Async.fromFuture(eff.delay(requestFuture)).adaptError { case _: IllegalUriException =>
            val error = StreamsNotificationsError.InvalidUriProvided(uri.toString())
            log.warn(error.getMessage)
            error
          }
        }
      }
    }
  }
}
