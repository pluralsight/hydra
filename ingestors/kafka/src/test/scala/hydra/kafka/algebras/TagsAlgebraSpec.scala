package hydra.kafka.algebras

import cats.effect.{Concurrent, ContextShift, IO, Sync, Timer}
import cats.implicits._
import hydra.kafka.algebras.KafkaClientAlgebra.PublishResponse
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import retry.RetryPolicies.{exponentialBackoff, limitRetries}
import retry.{RetryDetails, RetryPolicy}
import retry.syntax.all._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class TagsAlgebraSpec extends AnyWordSpecLike with Matchers {
  implicit private val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val concurrentEffect: Concurrent[IO] = IO.ioConcurrentEffect

  private implicit val policy: RetryPolicy[IO] = limitRetries[IO](5) |+| exponentialBackoff[IO](500.milliseconds)
  private implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)
  private implicit def noop[A]: (A, RetryDetails) => IO[Unit] = retry.noop[IO, A]

  implicit private def unsafeLogger[F[_]: Sync]: SelfAwareStructuredLogger[F] =
    Slf4jLogger.getLogger[F]

  private implicit class RetryAndAssert[A](boolIO: IO[A]) {
    def retryIfFalse(check: A => Boolean): IO[Assertion] =
      boolIO.map(check).retryingOnFailures(identity, policy, noop).map(assert(_))
  }

  (for {
    kafkaClientAlgebra <- KafkaClientAlgebra.test[IO]
    tagsAlgebra <- TagsAlgebra.make("_dvs.tags-topic", "tagsClient", kafkaClientAlgebra)
  } yield runTests(tagsAlgebra)
  ).unsafeRunSync()


  private def runTests(tagsAlgebra: TagsAlgebra[IO]): Unit = {
    "TagsAlgebraSpec" should {
      "Create a Tag" in {
        val tag = HydraTag("Source: blah", "This comes from blah")
        tagsAlgebra.createOrUpdateTag(tag)
          .unsafeRunSync shouldBe Right(PublishResponse(0,0))
      }
      "Create a duplicate Tag" in {
        val tag = HydraTag("Source: blah", "This comes from blah")
        tagsAlgebra.createOrUpdateTag(tag)
          .unsafeRunSync shouldBe Right(PublishResponse(0,1))
      }
      "Create a different Tag" in {
        val tag = HydraTag("Source: Bret", "This comes from Bret being on a call while I do this")
        tagsAlgebra.createOrUpdateTag(tag)
          .unsafeRunSync shouldBe Right(PublishResponse(0,2))
      }
      "Get Tags" in {
        tagsAlgebra.getAllTags.retryIfFalse(_.size == 2)
      }
      "return true when validating a good tag" in {
        tagsAlgebra.validateTags(List("Source: blah")).unsafeRunSync() shouldBe ()
        tagsAlgebra.validateTags(List("Source: Bret")).unsafeRunSync() shouldBe ()
        tagsAlgebra.validateTags(List("Source: blah", "Source: Bret")).unsafeRunSync() shouldBe ()
      }
      "return false when validating a bad tag" in {
        a [tagsAlgebra.TagsException] shouldBe thrownBy(tagsAlgebra.validateTags(List("Source: KSQL")).unsafeRunSync())
        a [tagsAlgebra.TagsException] shouldBe thrownBy(tagsAlgebra.validateTags(List("Source: blah", "Source: KSQL")).unsafeRunSync())
      }
    }
  }

}
