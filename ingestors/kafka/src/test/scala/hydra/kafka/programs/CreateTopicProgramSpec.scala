package hydra.kafka.programs

import java.time.Instant

import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ContextShift, IO, Sync, Timer}
import cats.implicits._
import hydra.avro.registry.SchemaRegistry
import hydra.avro.registry.SchemaRegistry.{SchemaId, SchemaVersion}
import hydra.core.marshallers.History
import hydra.kafka.algebras.KafkaAdminAlgebra.{Topic, TopicName}
import hydra.kafka.algebras.KafkaClientAlgebra.{ConsumerGroup, PublishError}
import hydra.kafka.algebras.{KafkaAdminAlgebra, KafkaClientAlgebra}
import hydra.kafka.model.ContactMethod.Email
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model._
import hydra.kafka.producer.StringRecord
import hydra.kafka.util.KafkaUtils.TopicDetails
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import retry.{RetryPolicies, RetryPolicy}

import scala.concurrent.ExecutionContext

class CreateTopicProgramSpec extends AnyWordSpecLike with Matchers {

  implicit private def unsafeLogger[F[_]: Sync]: SelfAwareStructuredLogger[F] =
    Slf4jLogger.getLogger[F]
  implicit val timer: Timer[IO] = IO.timer(concurrent.ExecutionContext.global)
  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val concurrentEffect: Concurrent[IO] = IO.ioConcurrentEffect


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
        kafka <- KafkaAdminAlgebra.test[IO]
        kafkaClient <- KafkaClientAlgebra.test[IO]
        registerInternalMetadata = new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
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
          override def getAllVersions(subject: String): IO[List[Int]] = IO.pure(List())
          override def getAllSubjects: IO[List[String]] = IO.pure(List())

          override def getSchemaRegistryClient: IO[SchemaRegistryClient] = IO.raiseError(new Exception("Something horrible went wrong!"))

          override def getLatestSchemaBySubject(subject: String): IO[Option[Schema]] = ???
        }
      (for {
        kafka <- KafkaAdminAlgebra.test[IO]
        kafkaClient <- KafkaClientAlgebra.test[IO]
        ref <- Ref[IO]
          .of(TestState(deleteSchemaWasCalled = false, 0))
        _ <- new CreateTopicProgram[IO](
          getSchemaRegistry(ref),
          kafka,
          kafkaClient,
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
          override def getAllVersions(subject: String): IO[List[Int]] = IO.pure(Nil)
          override def getAllSubjects: IO[List[String]] = IO.pure(Nil)
          override def getSchemaRegistryClient: IO[SchemaRegistryClient] = IO.raiseError(new Exception("Something horrible went wrong!"))

          override def getLatestSchemaBySubject(subject: String): IO[Option[Schema]] = ???
        }

      (for {
        kafka <- KafkaAdminAlgebra.test[IO]
        kafkaClient <- KafkaClientAlgebra.test[IO]
        ref <- Ref[IO].of(0)
        _ <- new CreateTopicProgram[IO](
          getSchemaRegistry(ref),
          kafka,
          kafkaClient,
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
          override def getAllVersions(subject: String): IO[List[Int]] = IO.pure(Nil)
          override def getAllSubjects: IO[List[String]] = IO.pure(Nil)
          override def getSchemaRegistryClient: IO[SchemaRegistryClient] = IO.raiseError(new Exception)

          override def getLatestSchemaBySubject(subject: String): IO[Option[Schema]] = ???
        }

      val schemaRegistryState = Map("subject-key" -> 1)
      (for {
        kafka <- KafkaAdminAlgebra.test[IO]
        kafkaClient <- KafkaClientAlgebra.test[IO]
        ref <- Ref[IO]
          .of(TestState(schemaRegistryState))
        _ <- new CreateTopicProgram[IO](
          getSchemaRegistry(ref),
          kafka,
          kafkaClient,
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
      (for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]
        kafkaClient <- KafkaClientAlgebra.test[IO]
        _ <- new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
          policy,
          Subject.createValidated("test-metadata-topic").get
        ).createTopic(
          createTopicMetadataRequest(subject, keySchema, valueSchema),
          TopicDetails(1, 1)
        )
        topic <- kafka.describeTopic(subject)
      } yield topic.get shouldBe Topic(subject, 1)).unsafeRunSync()
    }

    "ingest metadata into the metadata topic" in {
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      val subject = "subject"
      val metadataTopic = "test-metadata-topic"
      val request = createTopicMetadataRequest(subject, keySchema, valueSchema)
      val (key, value) = request.toKeyAndValue
      (for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafkaAdmin <- KafkaAdminAlgebra.test[IO]
        publishTo <- Ref[IO].of(Map.empty[String, (GenericRecord, Option[GenericRecord])])
        kafkaClient <- IO(
          new TestKafkaClientAlgebraWithPublishTo(publishTo)
        )
        m <- TopicMetadataV2.encode[IO](key, Some(value))
        _ <- new CreateTopicProgram[IO](
          schemaRegistry = schemaRegistry,
          kafkaAdmin = kafkaAdmin,
          kafkaClient = kafkaClient,
          retryPolicy = policy,
          v2MetadataTopicName = Subject.createValidated(metadataTopic).get
        ).createTopic(request, TopicDetails(1, 1))
        published <- publishTo.get
      } yield published shouldBe Map(metadataTopic -> (m._1, m._2))).unsafeRunSync()
    }

    "rollback kafka topic creation when error encountered in publishing metadata" in {
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      val subject = "subject"
      val metadataTopic = "test-metadata-topic"
      val request = createTopicMetadataRequest(subject, keySchema, valueSchema)
      (for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafkaAdmin <- KafkaAdminAlgebra.test[IO]
        publishTo <- Ref[IO].of(Map.empty[String, (GenericRecord, Option[GenericRecord])])
        kafkaClient <- IO(
          new TestKafkaClientAlgebraWithPublishTo(
            publishTo,
            failOnPublish = true
          )
        )
        _ <- new CreateTopicProgram[IO](
          schemaRegistry,
          kafkaAdmin,
          kafkaClient,
          policy,
          Subject.createValidated(metadataTopic).get
        ).createTopic(request, TopicDetails(1, 1)).attempt
        topic <- kafkaAdmin.describeTopic(subject)
      } yield topic should not be defined).unsafeRunSync()
    }

    "not delete an existing topic when rolling back" in {
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      val subject = "subject"
      val metadataTopic = "test-metadata-topic"
      val topicDetails = TopicDetails(1, 1)
      val request = createTopicMetadataRequest(subject, keySchema, valueSchema)
      (for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafkaAdmin <- KafkaAdminAlgebra.test[IO]
        publishTo <- Ref[IO].of(Map.empty[String, (GenericRecord, Option[GenericRecord])])
        kafkaClient <- IO(
          new TestKafkaClientAlgebraWithPublishTo(
            publishTo,
            failOnPublish = true
          )
        )
        _ <- kafkaAdmin.createTopic(subject, topicDetails)
        _ <- new CreateTopicProgram[IO](
          schemaRegistry,
          kafkaAdmin,
          kafkaClient,
          policy,
          Subject.createValidated(metadataTopic).get
        ).createTopic(request, topicDetails).attempt
        topic <- kafkaAdmin.describeTopic(subject)
      } yield topic shouldBe defined).unsafeRunSync()
    }

  }

  private final class TestKafkaClientAlgebraWithPublishTo(
                                                          publishTo: Ref[IO, Map[TopicName, (GenericRecord, Option[GenericRecord])]],
                                                          failOnPublish: Boolean = false
  ) extends KafkaClientAlgebra[IO] {

    override def publishMessage(record: (GenericRecord, Option[GenericRecord]), topicName: TopicName): IO[Either[PublishError, Unit]] =
      if (failOnPublish) {
        IO.pure(Left(PublishError.Timeout))
      } else {
        publishTo.update(_ + (topicName -> record)).attemptNarrow[PublishError]
      }

    override def consumeMessages(topicName: TopicName, consumerGroup: String): fs2.Stream[IO, (GenericRecord, Option[GenericRecord])] = fs2.Stream.empty

    override def publishStringKeyMessage(record: (Option[String], Option[GenericRecord]), topicName: TopicName): IO[Either[PublishError, Unit]] = ???

    override def consumeStringKeyMessages(topicName: TopicName, consumerGroup: ConsumerGroup): fs2.Stream[IO, (Option[String], Option[GenericRecord])] = ???
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
