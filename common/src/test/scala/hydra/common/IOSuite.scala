package hydra.common

import cats.syntax.all._
import cats.effect.{ContextShift, IO, Timer}
import org.scalatest.{Assertion, AsyncTestSuite}
import retry.RetryPolicies.{exponentialBackoff, limitRetries}
import retry.RetryPolicy
import scala.concurrent.duration._

import scala.concurrent.{ExecutionContext, Future}

trait IOSuite {
  _: AsyncTestSuite =>
  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  implicit val defaultRetryPolicy: RetryPolicy[IO] = limitRetries[IO](5) |+| exponentialBackoff[IO](500.milliseconds)

  implicit def ioToFutureAssertion(io: IO[Assertion]): Future[Assertion] = io.unsafeToFuture()
}
