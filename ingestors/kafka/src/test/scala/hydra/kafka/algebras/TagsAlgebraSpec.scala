package hydra.kafka.algebras

import cats.effect.{Concurrent, ContextShift, IO, Sync}
import hydra.kafka.algebras.KafkaClientAlgebra.PublishResponse
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContext

class TagsAlgebraSpec extends AnyWordSpecLike with Matchers {
  implicit private val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val concurrentEffect: Concurrent[IO] = IO.ioConcurrentEffect
  implicit private def unsafeLogger[F[_]: Sync]: SelfAwareStructuredLogger[F] =
    Slf4jLogger.getLogger[F]

  (for {
    kafkaClientAlgebra <- KafkaClientAlgebra.test[IO]
    tagsAlgebra <- TagsAlgebra.make("_dvs.tags-topic", "tagsClient", kafkaClientAlgebra)
  } yield (runTests(tagsAlgebra, kafkaClientAlgebra))
  ).unsafeRunSync()


  private def runTests(tagsAlgebra: TagsAlgebra[IO], kafkaClientAlgebra: KafkaClientAlgebra[IO]): Unit = {
    "TagsAlgebraSpec" should {
      "Create a Tag" in {
        val tag = HydraTag("Source: blah", "This comes from blah")
        tagsAlgebra.createOrUpdateTag(tag, kafkaClientAlgebra)
          .unsafeRunSync shouldBe Right(PublishResponse(0,0))
      }
      "Create a duplicate Tag" in {
        val tag = HydraTag("Source: blah", "This comes from blah")
        tagsAlgebra.createOrUpdateTag(tag, kafkaClientAlgebra)
          .unsafeRunSync shouldBe Right(PublishResponse(0,1))
      }
      "Create a different Tag" in {
        val tag = HydraTag("Source: Bret", "This comes from Bret being on a call while I do this")
        tagsAlgebra.createOrUpdateTag(tag, kafkaClientAlgebra)
          .unsafeRunSync shouldBe Right(PublishResponse(0,2))
      }
      "Get Tags" in {
        tagsAlgebra.getAllTags.unsafeRunSync() shouldBe List(HydraTag("Source: blah", "This comes from blah"),
          HydraTag("Source: Bret", "This comes from Bret being on a call while I do this"))
      }
    }
  }

}
