package hydra.common.util

import cats.effect.IO

import scala.concurrent.Future

trait Futurable[F[_]] {

  /**
    * This method is only to be used for interop with legacy `Future` based code.
    * @param f The effect
    * @tparam A The type the effect contains
    * @return A future of the type the effect contained
    */
  def unsafeToFuture[A](f: F[A]): Future[A]

}

object Futurable {
  def apply[F[_]](implicit f: Futurable[F]): Futurable[F] = f

  implicit val futurableIo: Futurable[IO] = new Futurable[IO] {
    override def unsafeToFuture[A](f: IO[A]): Future[A] = f.unsafeToFuture()
  }
}
