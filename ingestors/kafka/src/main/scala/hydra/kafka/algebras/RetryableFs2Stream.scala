package hydra.kafka.algebras

import cats.Monad
import cats.effect.ExitCase.{Canceled, Completed, Error}
import cats.effect.Sync
import cats.implicits._
import org.typelevel.log4cats.Logger
import hydra.common.alerting.AlertProtocol.NotificationMessage
import hydra.common.alerting.NotificationLevel
import hydra.common.alerting.sender.InternalNotificationSender
import hydra.kafka.algebras.HydraTag.StringJsonFormat

import scala.language.higherKinds

object RetryableFs2Stream {

  trait RetryPolicy extends Any {
    def count: Int
  }

  object RetryPolicy {
    case object Once extends RetryPolicy {
      override val count: Int = 1
    }

    case object Zero extends RetryPolicy {
      override val count: Int = 0
    }

    case object Infinite extends RetryPolicy {
      override val count: Int = -1
    }

    case class GivenCount(override val count: Int) extends AnyVal with RetryPolicy

    def apply(retryCount: Int): RetryPolicy = {
      if (retryCount < 0) Infinite
      else if (retryCount == 0) Zero
      else if (retryCount == 1) Once
      else GivenCount(retryCount)
    }
  }


  implicit class ReRunnableStreamAdder[F[_] : Monad : Sync : Logger, O](stream: fs2.Stream[F, O]) {

    import hydra.kafka.algebras.RetryableFs2Stream.RetryPolicy._

    /**
      * Reruns fs2 Streams when it accidentally or successfully stops
      */
    def makeRetryable(retryPolicy: RetryPolicy = Zero,
                      streamName: String,
                      onReRunAction: Option[Throwable] => F[Unit] = (_) => Sync[F].unit): fs2.Stream[F, O] = {
      def doOnRerun(error: Option[Throwable]) =
        if (retryPolicy.count != 0) {
          onReRunAction(error)
        } else {
          Sync[F].unit
        }

      stream.onFinalizeCase {
        case Completed | Canceled =>
          Logger[F].error(getStreamFinishedUnexpectedlyMessage(streamName, retryPolicy)) *>
            doOnRerun(None)
        case Error(error) =>
          Logger[F].error(error)(getStreamFailedMessage(streamName, retryPolicy)) *>
            doOnRerun(error.some)
      }
        .attempt
        .through(stream => {
          retryPolicy match {
            case Infinite => stream.repeat
            case Zero => stream
            case retryPolicy: RetryPolicy => stream.repeatN(retryPolicy.count)
          }
        }
          // skip already processed errors
          .collect { case Right(value) => value })
    }

    def makeRetryableWithNotification(retryPolicy: RetryPolicy = Zero, streamName: String)
                                     (implicit internalNotificationSender: InternalNotificationSender[F], monad: Monad[F]): fs2.Stream[F, O] =
      makeRetryable(retryPolicy, streamName, sendNotificationOnRetry[F](streamName, retryPolicy))
  }

  private def sendNotificationOnRetry[F[_] : Monad](streamName: String, retryPolicy: RetryPolicy)
                                                   (implicit internalNotificationSender: InternalNotificationSender[F]): Option[Throwable] => F[Unit] = {
    case Some(error: Throwable) => InternalNotificationSender(NotificationLevel.Error)
      .send(NotificationMessage(getStreamFailedMessage(streamName, retryPolicy), error.getMessage.some))
    case None => InternalNotificationSender(NotificationLevel.Error)
      .send(NotificationMessage(getStreamFinishedUnexpectedlyMessage(streamName, retryPolicy)))
  }

  private def getStreamFinishedUnexpectedlyMessage(streamName: String, retryPolicy: RetryPolicy) =
    s"`$streamName` Stream finished unexpectedly. It will be restarted due to RetryPolicy: $retryPolicy"

  private def getStreamFailedMessage(streamName: String, retryPolicy: RetryPolicy) =
    s"`$streamName` Stream failed. It will be restarted due to RetryPolicy: $retryPolicy"

}
