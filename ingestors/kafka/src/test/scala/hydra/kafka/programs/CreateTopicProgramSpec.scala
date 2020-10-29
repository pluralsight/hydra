package hydra.kafka.programs

import java.time.Instant

import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ContextShift, IO, Sync, Timer}
import cats.syntax.all._
import fs2.kafka._
import hydra.avro.registry.SchemaRegistry
import hydra.avro.registry.SchemaRegistry.{SchemaId, SchemaVersion}
import hydra.kafka.algebras.KafkaAdminAlgebra.{Topic, TopicName}
import hydra.kafka.algebras.KafkaClientAlgebra.{ConsumerGroup, Offset, Partition, PublishError, PublishResponse}
import hydra.kafka.algebras.MetadataAlgebra.TopicMetadataContainer
import hydra.kafka.algebras.{KafkaAdminAlgebra, KafkaClientAlgebra, MetadataAlgebra}
import hydra.kafka.model.ContactMethod.Email
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model._
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
  type Record = (GenericRecord, Option[GenericRecord], Option[Headers])

  private def metadataAlgebraF(
                                metadataTopic: String,
                                s: SchemaRegistry[IO],
                                k: KafkaClientAlgebra[IO]
                              ) = MetadataAlgebra.make(metadataTopic, "consumerGroup", k, s, consumeMetadataEnabled = true)

  private val keySchema = getSchema("key")
  private val valueSchema = getSchema("val")

  private def createTopicMetadataRequest(
      keySchema: Schema,
      valueSchema: Schema,
      email: String = "test@test.com",
      createdDate: Instant = Instant.now(),
      deprecated: Boolean = false,
      deprecatedDate: Option[Instant] = None
  ): TopicMetadataV2Request =
    TopicMetadataV2Request(
      Schemas(keySchema, valueSchema),
      StreamTypeV2.Entity,
      deprecated = deprecated,
      deprecatedDate,
      Public,
      NonEmptyList.of(Email.create(email).get),
      createdDate,
      List.empty,
      None,
      Some("dvs-teamName")
    )

  "CreateTopicSpec" must {
    "register the two avro schemas" in {
      val schemaRegistryIO = SchemaRegistry.test[IO]
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp

      (for {
        schemaRegistry <- schemaRegistryIO
        kafka <- KafkaAdminAlgebra.test[IO]
        kafkaClient <- KafkaClientAlgebra.test[IO]
        metadata <- metadataAlgebraF("dvs.v2Topic", schemaRegistry, kafkaClient)
        registerInternalMetadata = new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
          policy,
          Subject.createValidated("dvs.v2Topic").get,
          metadata
        )
        _ = registerInternalMetadata
          .createTopic(
            Subject.createValidated("dvs.subject").get,
            createTopicMetadataRequest(keySchema, valueSchema),
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

          override def getLatestSchemaBySubject(subject: String): IO[Option[Schema]] = IO.pure(None)

          override def getSchemaFor(subject: String, schemaVersion: SchemaVersion): IO[Option[Schema]] = IO.pure(None)
          override def deleteSchemaSubject(subject: String): IO[Unit] = IO.pure(())
        }
      (for {
        kafka <- KafkaAdminAlgebra.test[IO]
        kafkaClient <- KafkaClientAlgebra.test[IO]
        ref <- Ref[IO]
          .of(TestState(deleteSchemaWasCalled = false, 0))
        schemaRegistry = getSchemaRegistry(ref)
        metadata <- metadataAlgebraF("test", schemaRegistry, kafkaClient)
        _ <- new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
          policy,
          Subject.createValidated("dvs.test").get,
          metadata
        ).createTopic(
            Subject.createValidated("dvs.subject").get,
            createTopicMetadataRequest(keySchema, valueSchema),
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
          override def getLatestSchemaBySubject(subject: String): IO[Option[Schema]] = IO.pure(None)
          override def getSchemaFor(subject: String, schemaVersion: SchemaVersion): IO[Option[Schema]] = IO.pure(None)
          override def deleteSchemaSubject(subject: String): IO[Unit] = IO.pure(())
        }

      (for {
        kafka <- KafkaAdminAlgebra.test[IO]
        kafkaClient <- KafkaClientAlgebra.test[IO]
        ref <- Ref[IO].of(0)
        schemaRegistry = getSchemaRegistry(ref)
        metadata <- metadataAlgebraF("test", schemaRegistry, kafkaClient)
        _ <- new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
          policy,
          Subject.createValidated("dvs.test").get,
          metadata
        ).createTopic(
            Subject.createValidated("dvs.subject").get,
            createTopicMetadataRequest(keySchema, valueSchema),
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
          override def getLatestSchemaBySubject(subject: String): IO[Option[Schema]] = IO.pure(None)
          override def getSchemaFor(subject: String, schemaVersion: SchemaVersion): IO[Option[Schema]] = IO.pure(None)
          override def deleteSchemaSubject(subject: String): IO[Unit] = IO.pure(())
        }

      val schemaRegistryState = Map("subject-key" -> 1)
      (for {
        kafka <- KafkaAdminAlgebra.test[IO]
        kafkaClient <- KafkaClientAlgebra.test[IO]
        ref <- Ref[IO]
          .of(TestState(schemaRegistryState))
        schemaRegistry = getSchemaRegistry(ref)
        metadata <- metadataAlgebraF("test", schemaRegistry, kafkaClient)
        _ <- new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
          policy,
          Subject.createValidated("dvs.test").get,
          metadata
        ).createTopic(
            Subject.createValidated("dvs.subject").get,
            createTopicMetadataRequest(keySchema, valueSchema),
            TopicDetails(1, 1)
          )
          .attempt
        result <- ref.get
      } yield result.schemas shouldBe schemaRegistryState).unsafeRunSync()
    }

    "create the topic in Kafka" in {
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      val subject = "dvs.subject"
      (for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]
        kafkaClient <- KafkaClientAlgebra.test[IO]
        metadata <- metadataAlgebraF("dvs.test-metadata-topic", schemaRegistry, kafkaClient)
        _ <- new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
          policy,
          Subject.createValidated("dvs.test-metadata-topic").get,
          metadata
        ).createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchema, valueSchema),
          TopicDetails(1, 1)
        )
        topic <- kafka.describeTopic(subject)
      } yield topic.get shouldBe Topic(subject, 1)).unsafeRunSync()
    }

    "ingest metadata into the metadata topic" in {
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      val subject = Subject.createValidated("dvs.subject").get
      val metadataTopic = "dvs.test-metadata-topic"
      val request = createTopicMetadataRequest(keySchema, valueSchema)
      val key = TopicMetadataV2Key(subject)
      val value = request.toValue
      (for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafkaAdmin <- KafkaAdminAlgebra.test[IO]
        publishTo <- Ref[IO].of(Map.empty[String, (GenericRecord, Option[GenericRecord], Option[Headers])])
        kafkaClient <- IO(
          new TestKafkaClientAlgebraWithPublishTo(publishTo)
        )
        m <- TopicMetadataV2.encode[IO](key, Some(value))
        metadata <- metadataAlgebraF(metadataTopic, schemaRegistry, kafkaClient)
        _ <- new CreateTopicProgram[IO](
          schemaRegistry = schemaRegistry,
          kafkaAdmin = kafkaAdmin,
          kafkaClient = kafkaClient,
          retryPolicy = policy,
          v2MetadataTopicName = Subject.createValidated(metadataTopic).get,
          metadata
        ).createTopic(subject, request, TopicDetails(1, 1))
        published <- publishTo.get
      } yield published shouldBe Map(metadataTopic -> (m._1, m._2, None))).unsafeRunSync()
    }

    "ingest updated metadata into the metadata topic - verify created date did not change" in {
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      val subject = Subject.createValidated("dvs.subject").get
      val metadataTopic = "dvs.test-metadata-topic"
      val request = createTopicMetadataRequest(keySchema, valueSchema)
      val key = TopicMetadataV2Key(subject)
      val value = request.toValue
      val updatedRequest = createTopicMetadataRequest(keySchema, valueSchema, "updated@email.com", Instant.ofEpochSecond(0))
      val updatedKey = TopicMetadataV2Key(subject)
      val updatedValue = updatedRequest.toValue
      (for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafkaAdmin <- KafkaAdminAlgebra.test[IO]
        publishTo <- Ref[IO].of(Map.empty[String, (GenericRecord, Option[GenericRecord], Option[Headers])])
        kafkaClient <- IO(
          new TestKafkaClientAlgebraWithPublishTo(publishTo)
        )
        consumeFrom <- Ref[IO].of(Map.empty[Subject, TopicMetadataContainer])
        metadata <- IO(new TestMetadataAlgebraWithPublishTo(consumeFrom))
        m <- TopicMetadataV2.encode[IO](key, Some(value))
        updatedM <- TopicMetadataV2.encode[IO](updatedKey, Some(updatedValue.copy(createdDate = value.createdDate)))
        createTopicProgram = new CreateTopicProgram[IO](
          schemaRegistry = schemaRegistry,
          kafkaAdmin = kafkaAdmin,
          kafkaClient = kafkaClient,
          retryPolicy = policy,
          v2MetadataTopicName = Subject.createValidated(metadataTopic).get,
          metadata
        )
        _ <- createTopicProgram.createTopic(subject, request, TopicDetails(1, 1))
        _ <- metadata.addToMetadata(subject, request)
        metadataMap <- publishTo.get
        _ <- createTopicProgram.createTopic(subject, updatedRequest, TopicDetails(1, 1))
        updatedMap <- publishTo.get
      } yield {
        metadataMap shouldBe Map(metadataTopic -> (m._1, m._2, None))
        updatedMap shouldBe Map(metadataTopic -> (updatedM._1, updatedM._2, None))
      }).unsafeRunSync()
    }

    "rollback kafka topic creation when error encountered in publishing metadata" in {
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      val subject = "dvs.subject"
      val metadataTopic = "dvs.test-metadata-topic"
      val request = createTopicMetadataRequest(keySchema, valueSchema)
      (for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafkaAdmin <- KafkaAdminAlgebra.test[IO]
        publishTo <- Ref[IO].of(Map.empty[String, (GenericRecord, Option[GenericRecord], Option[Headers])])
        kafkaClient <- IO(
          new TestKafkaClientAlgebraWithPublishTo(
            publishTo,
            failOnPublish = true
          )
        )
        metadata <- metadataAlgebraF(metadataTopic, schemaRegistry, kafkaClient)
        _ <- new CreateTopicProgram[IO](
          schemaRegistry,
          kafkaAdmin,
          kafkaClient,
          policy,
          Subject.createValidated(metadataTopic).get,
          metadata
        ).createTopic(Subject.createValidated(subject).get, request, TopicDetails(1, 1)).attempt
        topic <- kafkaAdmin.describeTopic(subject)
      } yield topic should not be defined).unsafeRunSync()
    }

    "not delete an existing topic when rolling back" in {
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      val subject = "dvs.subject"
      val metadataTopic = "dvs.test-metadata-topic"
      val topicDetails = TopicDetails(1, 1)
      val request = createTopicMetadataRequest(keySchema, valueSchema)
      (for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafkaAdmin <- KafkaAdminAlgebra.test[IO]
        publishTo <- Ref[IO].of(Map.empty[String, (GenericRecord, Option[GenericRecord], Option[Headers])])
        kafkaClient <- IO(
          new TestKafkaClientAlgebraWithPublishTo(
            publishTo,
            failOnPublish = true
          )
        )
        _ <- kafkaAdmin.createTopic(subject, topicDetails)
        metadata <- metadataAlgebraF(metadataTopic, schemaRegistry, kafkaClient)
        _ <- new CreateTopicProgram[IO](
          schemaRegistry,
          kafkaAdmin,
          kafkaClient,
          policy,
          Subject.createValidated(metadataTopic).get,
          metadata
        ).createTopic(Subject.createValidated(subject).get, request, topicDetails).attempt
        topic <- kafkaAdmin.describeTopic(subject)
      } yield topic shouldBe defined).unsafeRunSync()
    }

    "ingest updated metadata into the metadata topic - verify deprecated date if supplied is not overwritten" in {
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      val subject = Subject.createValidated("dvs.subject").get
      val metadataTopic = "_test.metadata-topic"
      val request = createTopicMetadataRequest(keySchema, valueSchema, deprecated = true, deprecatedDate = Some(Instant.now))
      val updatedRequest = createTopicMetadataRequest(keySchema, valueSchema, "updated@email.com", deprecated = true)
      (for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafkaAdmin <- KafkaAdminAlgebra.test[IO]
        publishTo <- Ref[IO].of(Map.empty[String, (GenericRecord, Option[GenericRecord], Option[Headers])])
        kafkaClient <- IO(
          new TestKafkaClientAlgebraWithPublishTo(publishTo)
        )
        consumeFrom <- Ref[IO].of(Map.empty[Subject, TopicMetadataContainer])
        metadata <- IO(new TestMetadataAlgebraWithPublishTo(consumeFrom))
        createTopicProgram = new CreateTopicProgram[IO](
          schemaRegistry = schemaRegistry,
          kafkaAdmin = kafkaAdmin,
          kafkaClient = kafkaClient,
          retryPolicy = policy,
          v2MetadataTopicName = Subject.createValidated(metadataTopic).get,
          metadata
        )
        _ <- createTopicProgram.createTopic(subject, request, TopicDetails(1, 1))
        _ <- metadata.addToMetadata(subject, request)
        metadataMap <- publishTo.get
        _ <- createTopicProgram.createTopic(subject, updatedRequest, TopicDetails(1, 1))
        updatedMap <- publishTo.get
      } yield {
        val dd = metadataMap.get(metadataTopic).get._2.get.get("deprecatedDate")
        val ud = updatedMap.get(metadataTopic).get._2.get.get("deprecatedDate")
        ud shouldBe dd
      }).unsafeRunSync()
    }

    "ingest updated metadata into the metadata topic - verify deprecated date is updated when starting with None" in {
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      val subject = Subject.createValidated("dvs.subject").get
      val metadataTopic = "_test.metadata-topic"
      val request = createTopicMetadataRequest(keySchema, valueSchema)
      val updatedRequest = createTopicMetadataRequest(keySchema, valueSchema, "updated@email.com", deprecated = true)
      (for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafkaAdmin <- KafkaAdminAlgebra.test[IO]
        publishTo <- Ref[IO].of(Map.empty[String, (GenericRecord, Option[GenericRecord], Option[Headers])])
        kafkaClient <- IO(
          new TestKafkaClientAlgebraWithPublishTo(publishTo)
        )
        consumeFrom <- Ref[IO].of(Map.empty[Subject, TopicMetadataContainer])
        metadata <- IO(new TestMetadataAlgebraWithPublishTo(consumeFrom))
        createTopicProgram = new CreateTopicProgram[IO](
          schemaRegistry = schemaRegistry,
          kafkaAdmin = kafkaAdmin,
          kafkaClient = kafkaClient,
          retryPolicy = policy,
          v2MetadataTopicName = Subject.createValidated(metadataTopic).get,
          metadata
        )
        _ <- createTopicProgram.createTopic(subject, request, TopicDetails(1, 1))
        _ <- metadata.addToMetadata(subject, request)
        metadataMap <- publishTo.get
        _ <- createTopicProgram.createTopic(subject, updatedRequest, TopicDetails(1, 1))
        updatedMap <- publishTo.get
      } yield {
        val ud = metadataMap.get(metadataTopic).get._2.get.get("deprecatedDate")
        val dd = updatedMap.get(metadataTopic).get._2.get.get("deprecatedDate")
        ud shouldBe null
        Instant.parse(dd.toString) shouldBe a[Instant]
      }).unsafeRunSync()
    }

  }

  private final class TestKafkaClientAlgebraWithPublishTo(
                                                          publishTo: Ref[IO, Map[TopicName, Record]],
                                                          failOnPublish: Boolean = false
  ) extends KafkaClientAlgebra[IO] {

    override def publishMessage(record: Record, topicName: TopicName): IO[Either[PublishError, PublishResponse]] =
      if (failOnPublish) {
        IO.pure(Left(PublishError.Timeout))
      } else {
        publishTo.update(_ + (topicName -> record)).map(_ => PublishResponse(0, 0)).attemptNarrow[PublishError]
      }

    override def consumeMessages(topicName: TopicName, consumerGroup: String, commitOffsets: Boolean): fs2.Stream[IO, Record] = fs2.Stream.empty

    override def publishStringKeyMessage(record: (Option[String], Option[GenericRecord], Option[Headers]), topicName: TopicName): IO[Either[PublishError, PublishResponse]] = ???

    override def consumeStringKeyMessages(topicName: TopicName, consumerGroup: ConsumerGroup, commitOffsets: Boolean): fs2.Stream[IO, (Option[String], Option[GenericRecord], Option[Headers])] = ???

    override def withProducerRecordSizeLimit(sizeLimitBytes: Long): IO[KafkaClientAlgebra[IO]] = ???

    override def consumeMessagesWithOffsetInfo(topicName: TopicName, consumerGroup: ConsumerGroup, commitOffsets: Boolean): fs2.Stream[IO, ((GenericRecord, Option[GenericRecord], Option[Headers]), (Partition, Offset))] = fs2.Stream.empty
  }

  private final class TestMetadataAlgebraWithPublishTo(consumeFrom: Ref[IO, Map[Subject, TopicMetadataContainer]]) extends MetadataAlgebra[IO] {
    override def getMetadataFor(subject: Subject): IO[Option[MetadataAlgebra.TopicMetadataContainer]] = consumeFrom.get.map(_.get(subject))

    override def getAllMetadata: IO[List[MetadataAlgebra.TopicMetadataContainer]] = ???

    def addToMetadata(subject: Subject, t: TopicMetadataV2Request): IO[Unit] =
      consumeFrom.update(_ + (subject -> TopicMetadataContainer(TopicMetadataV2Key(subject), t.toValue, None, None)))
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
