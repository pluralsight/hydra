package hydra.kafka.algebras

import cats.effect.Sync
import cats.implicits._
import io.chrisdavenport.log4cats.Logger

import scala.language.higherKinds

object RetryableFs2Stream {

  trait RetryPolicy extends Any {
    def count: Int
  }

  object RetryPolicy {
    case object Once extends AnyVal with RetryPolicy {
      val count: Int = 1
    }

    case object Zero extends AnyVal with  RetryPolicy {
      val count: Int = 0
    }

    case object Infinite extends AnyVal with RetryPolicy {
      val count: Int = -1
    }

    case class GivenCount(count: Int) extends AnyVal with  RetryPolicy

    def apply(retryCount: Int): RetryPolicy = {
      if (retryCount < 0) Infinite
      else if (retryCount == 0) Zero
      else if (retryCount == 1) Once
      else GivenCount(retryCount)
    }
  }


  implicit class ReRunnableStreamAdder[F[_] : Sync : Logger, O](stream: fs2.Stream[F, O]) {

    import hydra.kafka.algebras.RetryableFs2Stream.RetryPolicy._

    /**
      * Reruns fs2 Streams when it accidentally or successfully stops
      */
    def makeRetryable(retryPolicy: RetryPolicy = Zero, onReRunAction: F[Unit] = Sync[F].unit)(onErrorMessage: String): fs2.Stream[F, O] = {
      stream.onComplete(
        fs2.Stream.eval(
          onReRunAction
        ) *> fs2.Stream.empty
      ).onError {
        case error => fs2.Stream.eval(Logger[F].error(error)(s"$onErrorMessage. Stream will be restarted due to RetryPolicy: $retryPolicy"))
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

  }
}
