package hydra.kafka

import scala.concurrent.duration._
import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.implicits._
import org.scalatest.{Assertion, AsyncTestSuite}
import retry.RetryPolicies.{exponentialBackoff, limitRetries}
import retry.RetryPolicy

import scala.concurrent.{ExecutionContext, Future}

trait IOSuite { _: AsyncTestSuite =>
  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  implicit val defaultRetryPolicy: RetryPolicy[IO] = limitRetries[IO](5) |+| exponentialBackoff[IO](500.milliseconds)

  implicit def ioToFutureAssertion(io: IO[Assertion]): Future[Assertion] = io.unsafeToFuture()

  implicit def resourceToFutureAssertion(resource: Resource[IO, Assertion]): Future[Assertion] = resource.allocated.unsafeToFuture().map(_._1)
}
