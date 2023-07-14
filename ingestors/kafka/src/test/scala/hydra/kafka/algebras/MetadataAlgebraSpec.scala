package hydra.kafka.algebras

import cats.data.NonEmptyList
import cats.effect.{Concurrent, ContextShift, IO, Sync, Timer}
import cats.syntax.all._
import fs2.kafka.Headers
import hydra.avro.registry.SchemaRegistry
import hydra.common.NotificationsTestSuite
import hydra.common.alerting.sender.InternalNotificationSender
import hydra.kafka.algebras.MetadataAlgebra.TopicMetadataContainer
import hydra.kafka.algebras.RetryableFs2Stream.RetryPolicy.Once
import hydra.kafka.model.ContactMethod.Slack
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model._
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.apache.avro.generic.GenericRecord
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import retry.RetryPolicies._
import retry._
import retry.syntax.all._

import java.time.Instant
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class MetadataAlgebraSpec extends AnyWordSpecLike with Matchers with NotificationsTestSuite {

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


  private val metadataTopicName = "_internal.metadataTopic"
  private val consumerGroup = "Consumer Group"

  {
    implicit val notificationSenderMock: InternalNotificationSender[IO] = getInternalNotificationSenderMock[IO]
    for {
    kafkaClient <- KafkaClientAlgebra.test[IO]
    schemaRegistry <- SchemaRegistry.test[IO]
    metadata <- MetadataAlgebra.make(Subject.createValidated(metadataTopicName).get, consumerGroup, kafkaClient, schemaRegistry, consumeMetadataEnabled = true, Once)
  } yield {
    runTests(metadata, kafkaClient)
  }}.unsafeRunSync()

  private def runTests(metadataAlgebra: MetadataAlgebra[IO], kafkaClientAlgebra: KafkaClientAlgebra[IO]): Unit = {
    "MetadataAlgebraSpec" should {

      "retrieve none for non-existant topic" in {
        val subject = Subject.createValidated("dvs.Non-existantTopic").get
        metadataAlgebra.getMetadataFor(subject).unsafeRunSync() shouldBe None
      }

      "retrieve metadata" in {
        val subject = Subject.createValidated("dvs.subject1").get
        val (genericRecordsIO, key, value) = getMetadataGenericRecords(subject)

        (for {
          record <- genericRecordsIO
          _ <- kafkaClientAlgebra.publishMessage(record, metadataTopicName)
          _ <- metadataAlgebra.getMetadataFor(subject).retryIfFalse(_.isDefined)
          metadata <- metadataAlgebra.getMetadataFor(subject)
        } yield metadata shouldBe Some(TopicMetadataContainer(key, value, None, None))).unsafeRunSync()
      }

      "retrieve all metadata" in {
        val subject = Subject.createValidated("dvs.subject2").get
        val (genericRecordsIO, key, value) = getMetadataGenericRecords(subject)
        (for {
          record <- genericRecordsIO
          _ <- kafkaClientAlgebra.publishMessage(record, metadataTopicName)
          _ <- metadataAlgebra.getMetadataFor(subject).retryIfFalse(_.isDefined)
          allMetadata <- metadataAlgebra.getAllMetadata
        } yield allMetadata should have length 2).unsafeRunSync()
      }

      "deletes topic from metadata" in {
        val subject = Subject.createValidated("dvs.subject1").get
        val (genericRecordsIO, key, value) = getMetadataGenericRecords(subject)
        val (genericRecordsIOWithoutValue, _, _) = getMetadataGenericRecords(subject, nullValue = true)

        (for {
          record <- genericRecordsIO
          nullRecord <- genericRecordsIOWithoutValue
          _ <- kafkaClientAlgebra.publishMessage(record, metadataTopicName)
          _ <- metadataAlgebra.getMetadataFor(subject).retryIfFalse(_.isDefined)
          metadata1 <- metadataAlgebra.getAllMetadata
          _ <- kafkaClientAlgebra.publishMessage(nullRecord, metadataTopicName)
          _ <- metadataAlgebra.getMetadataFor(subject).retryIfFalse(_.isEmpty)
          metadata <- metadataAlgebra.getMetadataFor(subject)
          metadata2 <- metadataAlgebra.getAllMetadata
        } yield {
          metadata shouldBe None
          metadata1.length shouldBe (metadata2.length + 1)
          assert(metadata1.nonEmpty)
        }).unsafeRunSync()
      }
    }
  }

  private def getMetadataGenericRecords(subject: Subject, nullValue: Boolean = false): (IO[(GenericRecord, Option[GenericRecord], Option[Headers])], TopicMetadataV2Key, TopicMetadataV2Value) = {
    val key = TopicMetadataV2Key(subject)
    val value = TopicMetadataV2Value(
        StreamTypeV2.Entity,
        deprecated = false,
        None,
        None,
        None,
        Public,
        NonEmptyList.one(Slack.create("#channel").get),
        Instant.now,
        List(),
        None,
        Some("dvs-teamName"),
        List.empty,
        Some("notificationUrl")
        )
    (TopicMetadataV2.encode[IO](key, if (nullValue) None else Some(value), None), key, value)
  }
}
