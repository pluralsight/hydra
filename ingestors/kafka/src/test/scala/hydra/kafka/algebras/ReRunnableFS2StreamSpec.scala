package hydra.kafka.algebras

import cats.effect.concurrent.Ref
import cats.effect.{IO, Sync}
import hydra.kafka.algebras.ReRunnableFS2StreamSpec.{erroredStream, successStream}
import hydra.kafka.algebras.RetryableFs2Stream.RetryPolicy.{Once, Zero}
import hydra.kafka.algebras.RetryableFs2Stream.{ReRunnableStreamAdder, _}
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.language.higherKinds

class ReRunnableFS2StreamSpec
  extends AnyWordSpecLike
    with Matchers {

  private implicit val catsLogger: SelfAwareStructuredLogger[IO] =
    Slf4jLogger.getLogger[IO]

  "ReRunnableFS2Stream" should {

    "Stream should restart if error occurred" in {
      val countRef = retryableStreamForTest(erroredStream, Once).unsafeRunSync()
      countRef.get.unsafeRunSync() shouldBe (1)
    }

    "Stream should restart if it is completed" in {
      val countRef = retryableStreamForTest(successStream, Once).unsafeRunSync()
      countRef.get.unsafeRunSync() shouldBe (1)
    }

    "Stream should retry given count of time" in {
      val countRef = retryableStreamForTest(successStream, RetryPolicy(12)).unsafeRunSync()
      countRef.get.unsafeRunSync() shouldBe (12)
    }

    "Stream shouldn`t retry if given Zero RetryPolicy" in {
      val countRef = retryableStreamForTest(successStream, Zero).unsafeRunSync()
      countRef.get.unsafeRunSync() shouldBe (0)
    }

    def retryableStreamForTest[O](stream: fs2.Stream[IO, O], retryPolicy: RetryPolicy): IO[Ref[IO, Int]] =
      for {
        retryRef <- Ref.of[IO, Int](0)
        _ <- stream.makeRetryable(retryPolicy, _ => retryRef.update(_ + 1))
          .compile.drain
      } yield retryRef

  }

}

object ReRunnableFS2StreamSpec {

  private val successStream = fs2.Stream.evalSeq(IO.pure(List(2, 1, 3, 5, 6, 7, 8, 9))).covary[IO]
  private val erroredStream = fs2.Stream.repeatEval(
    IO.raiseError(new RuntimeException("Expected error!"))
  )


}
