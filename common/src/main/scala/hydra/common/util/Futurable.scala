package hydra.common.util

import cats.effect.IO

import scala.concurrent.Future

trait Futurable[F[_], A] {
  def unsafeToFuture(f: F[A]): Future[A]
}

object Futurable {

  implicit def ioFuturable[A]: Futurable[IO, A] = (f: IO[A]) => f.unsafeToFuture()

}
