package hydra.kafka.programs

import java.time.Instant

import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.{IO, Sync, Timer}
import cats.implicits._
import hydra.avro.registry.SchemaRegistry
import hydra.avro.registry.SchemaRegistry.{SchemaId, SchemaVersion}
import hydra.core.marshallers.History
import hydra.kafka.model.ContactMethod.Email
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model.{Public, Schemas, TopicMetadataV2Request}
import hydra.kafka.util.KafkaClient
import hydra.kafka.util.KafkaClient.Topic
import hydra.kafka.util.KafkaUtils.TopicDetails
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import kafka.admin.TopicCommand.TopicDescription
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.{Matchers, WordSpec}
import retry.{RetryPolicies, RetryPolicy}

class CreateTopicSpec extends WordSpec with Matchers {

  implicit private def unsafeLogger[F[_]: Sync]: SelfAwareStructuredLogger[F] =
    Slf4jLogger.getLogger[F]
  implicit val timer: Timer[IO] = IO.timer(concurrent.ExecutionContext.global)

  private val keySchema = getSchema("key")
  private val valueSchema = getSchema("val")

  private def createTopicMetadataRequest(
      subject: String,
      keySchema: Schema,
      valueSchema: Schema
  ): TopicMetadataV2Request =
    TopicMetadataV2Request(
      Subject.createValidated(subject).get,
      Schemas(keySchema, valueSchema),
      History,
      deprecated = false,
      Public,
      NonEmptyList.of(Email.create("test@test.com").get),
      Instant.now,
      List.empty,
      None
    )

  "CreateTopicSpec" must {
    "register the two avro schemas" in {
      val schemaRegistryIO = SchemaRegistry.test[IO]
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp

      (for {
        schemaRegistry <- schemaRegistryIO
        kafka <- KafkaClient.test[IO]
        registerInternalMetadata = new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          policy,
          Subject.createValidated("v2Topic").get
        )
        _ = registerInternalMetadata
          .createTopic(
            createTopicMetadataRequest("subject", keySchema, valueSchema),
            TopicDetails(1, 1)
          )
          .unsafeRunSync()
        containsSingleKeyAndValue <- schemaRegistry.getAllSubjects.map(
          _.length == 2
        )
      } yield assert(containsSingleKeyAndValue)).unsafeRunSync()
    }

    "rollback schema creation on error" in {
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp

      case class TestState(
          deleteSchemaWasCalled: Boolean,
          numSchemasRegistered: Int
      )

      def getSchemaRegistry(ref: Ref[IO, TestState]): SchemaRegistry[IO] =
        new SchemaRegistry[IO] {
          override def registerSchema(
              subject: String,
              schema: Schema
          ): IO[SchemaId] = ref.get.flatMap {
            case TestState(_, 1) =>
              IO.raiseError(new Exception("Something horrible went wrong!"))
            case t: TestState =>
              val schemaId = t.numSchemasRegistered + 1
              ref.set(t.copy(numSchemasRegistered = schemaId)) *> IO.pure(
                schemaId
              )
          }
          override def deleteSchemaOfVersion(
              subject: String,
              version: SchemaVersion
          ): IO[Unit] = ref.update(_.copy(deleteSchemaWasCalled = true))
          override def getVersion(
              subject: String,
              schema: Schema
          ): IO[SchemaVersion] = ref.get.map { testState =>
            testState.numSchemasRegistered + 1
          }
          override def getAllVersions(subject: String): IO[List[Int]] = ???
          override def getAllSubjects: IO[List[String]] = ???
        }
      (for {
        kafka <- KafkaClient.test[IO]
        ref <- Ref[IO]
          .of(TestState(deleteSchemaWasCalled = false, 0))
        _ <- new CreateTopicProgram[IO](
          getSchemaRegistry(ref),
          kafka,
          policy,
          Subject.createValidated("test").get
        ).createTopic(
            createTopicMetadataRequest("subject", keySchema, valueSchema),
            TopicDetails(1, 1)
          )
          .attempt
        result <- ref.get
      } yield assert(result.deleteSchemaWasCalled)).unsafeRunSync()
    }

    "retry given number of attempts" in {
      val numberRetries = 3
      val policy: RetryPolicy[IO] = RetryPolicies.limitRetries(numberRetries)

      def getSchemaRegistry(ref: Ref[IO, Int]): SchemaRegistry[IO] =
        new SchemaRegistry[IO] {
          override def registerSchema(
              subject: String,
              schema: Schema
          ): IO[SchemaId] = ref.get.flatMap { n =>
            ref.set(n + 1) *> IO.raiseError(
              new Exception("Something horrible went wrong!")
            )
          }
          override def deleteSchemaOfVersion(
              subject: String,
              version: SchemaVersion
          ): IO[Unit] = IO.unit
          override def getVersion(
              subject: String,
              schema: Schema
          ): IO[SchemaVersion] = IO.pure(1)
          override def getAllVersions(subject: String): IO[List[Int]] = ???
          override def getAllSubjects: IO[List[String]] = ???
        }

      (for {
        kafka <- KafkaClient.test[IO]
        ref <- Ref[IO].of(0)
        _ <- new CreateTopicProgram[IO](
          getSchemaRegistry(ref),
          kafka,
          policy,
          Subject.createValidated("test").get
        ).createTopic(
            createTopicMetadataRequest("subject", keySchema, valueSchema),
            TopicDetails(1, 1)
          )
          .attempt
        result <- ref.get
      } yield result shouldBe numberRetries + 1).unsafeRunSync()
    }

    "not remove existing schemas on rollback" in {
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp

      type SchemaName = String
      case class TestState(schemas: Map[SchemaName, SchemaVersion])

      def getSchemaRegistry(ref: Ref[IO, TestState]): SchemaRegistry[IO] =
        new SchemaRegistry[IO] {
          override def registerSchema(
              subject: String,
              schema: Schema
          ): IO[SchemaId] = ref.get.flatMap { ts =>
            if (subject.contains("-value")) {
              IO.raiseError(new Exception)
            } else {
              IO.pure(ts.schemas(subject))
            }
          }
          override def deleteSchemaOfVersion(
              subject: String,
              version: SchemaVersion
          ): IO[Unit] =
            ref.update(ts => ts.copy(schemas = ts.schemas - subject))
          override def getVersion(
              subject: String,
              schema: Schema
          ): IO[SchemaVersion] = ref.get.map(_.schemas(subject))
          override def getAllVersions(subject: String): IO[List[Int]] = ???
          override def getAllSubjects: IO[List[String]] = ???
        }

      val schemaRegistryState = Map("subject-key" -> 1)
      (for {
        kafka <- KafkaClient.test[IO]
        ref <- Ref[IO]
          .of(TestState(schemaRegistryState))
        _ <- new CreateTopicProgram[IO](
          getSchemaRegistry(ref),
          kafka,
          policy,
          Subject.createValidated("test").get
        ).createTopic(
            createTopicMetadataRequest("subject", keySchema, valueSchema),
            TopicDetails(1, 1)
          )
          .attempt
        result <- ref.get
      } yield result.schemas shouldBe schemaRegistryState).unsafeRunSync()
    }

    "create the topic in Kafka" in {
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      val subject = "subject"
      for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafkaClient <- KafkaClient.test[IO]
        _ <- new CreateTopicProgram[IO](
          schemaRegistry,
          kafkaClient,
          policy,
          Subject.createValidated("test-metadata-topic").get).createTopic(
          createTopicMetadataRequest(subject, keySchema, valueSchema),
          TopicDetails(1, 1))
        topic <- kafkaClient.describeTopic(subject)
      } yield topic.get shouldBe Topic(subject, 1)
    }

    "ingest metadata into the metadata topic" in {}

  }

  private def getSchema(name: String): Schema =
    SchemaBuilder
      .record(name)
      .fields()
      .name("isTrue")
      .`type`()
      .stringType()
      .noDefault()
      .endRecord()
}
