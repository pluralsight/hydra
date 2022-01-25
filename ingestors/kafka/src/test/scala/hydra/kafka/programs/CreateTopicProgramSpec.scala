package hydra.kafka.programs

import java.time.Instant
import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.{Bracket, Concurrent, ContextShift, IO, Resource, Sync, Timer}
import cats.syntax.all._
import fs2.kafka._
import hydra.avro.registry.SchemaRegistry
import hydra.avro.registry.SchemaRegistry.{SchemaId, SchemaVersion}
import hydra.kafka.algebras.KafkaAdminAlgebra.{Topic, TopicName}
import hydra.kafka.algebras.KafkaClientAlgebra.{ConsumerGroup, Offset, Partition, PublishError, PublishResponse, TopicName}
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
import eu.timepit.refined._

import scala.concurrent.ExecutionContext
import hydra.kafka.model.TopicMetadataV2Request.NumPartitions
import hydra.kafka.programs.CreateTopicProgram._
import org.apache.kafka.common.TopicPartition
import org.scalatest.compatible.Assertion

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
                              ) = MetadataAlgebra.make(Subject.createValidated(metadataTopic).get, "consumerGroup", k, s, consumeMetadataEnabled = true)

  private val keySchema = getSchema("key")
  private val valueSchema = getSchema("val")

  private def createTopicMetadataRequest(
    keySchema: Schema,
    valueSchema: Schema,
    email: String = "test@test.com",
    createdDate: Instant = Instant.now(),
    deprecated: Boolean = false,
    deprecatedDate: Option[Instant] = None,
    numPartitions: Option[NumPartitions] = None,
    tags: Option[Map[String,String]] = None
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
      Some("dvs-teamName"),
      numPartitions,
      List.empty
    )

  private def createEventStreamTypeTopicMetadataRequest(
                                          keySchema: Schema,
                                          valueSchema: Schema,
                                          email: String = "test@test.com",
                                          createdDate: Instant = Instant.now(),
                                          deprecated: Boolean = false,
                                          deprecatedDate: Option[Instant] = None,
                                          numPartitions: Option[NumPartitions] = None,
                                          tags: Option[Map[String,String]] = None
                                        ): TopicMetadataV2Request =
    TopicMetadataV2Request(
      Schemas(keySchema, valueSchema),
      StreamTypeV2.Event,
      deprecated = deprecated,
      deprecatedDate,
      Public,
      NonEmptyList.of(Email.create(email).get),
      createdDate,
      List.empty,
      None,
      Some("dvs-teamName"),
      numPartitions,
      List.empty
    )

  "CreateTopicSpec" must {
    "register the two avro schemas" in {
      val schemaRegistryIO = SchemaRegistry.test[IO]
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp

      (for {
        schemaRegistry <- schemaRegistryIO
        kafka <- KafkaAdminAlgebra.test[IO]()
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
            TopicDetails(1, 1, 1)
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
        kafka <- KafkaAdminAlgebra.test[IO]()
        kafkaClient <- KafkaClientAlgebra.test[IO]
        ref <- Ref[IO]
          .of(TestState(deleteSchemaWasCalled = false, 0))
        schemaRegistry = getSchemaRegistry(ref)
        metadata <- metadataAlgebraF("_test.name", schemaRegistry, kafkaClient)
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
          TopicDetails(1, 1, 1)
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
        kafka <- KafkaAdminAlgebra.test[IO]()
        kafkaClient <- KafkaClientAlgebra.test[IO]
        ref <- Ref[IO].of(0)
        schemaRegistry = getSchemaRegistry(ref)
        metadata <- metadataAlgebraF("_test.name", schemaRegistry, kafkaClient)
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
          TopicDetails(1, 1, 1)
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
        kafka <- KafkaAdminAlgebra.test[IO]()
        kafkaClient <- KafkaClientAlgebra.test[IO]
        ref <- Ref[IO]
          .of(TestState(schemaRegistryState))
        schemaRegistry = getSchemaRegistry(ref)
        metadata <- metadataAlgebraF("_test.name", schemaRegistry, kafkaClient)
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
          TopicDetails(1, 1, 1)
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
        kafka <- KafkaAdminAlgebra.test[IO]()
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
          TopicDetails(1, 1, 1)
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
        kafkaAdmin <- KafkaAdminAlgebra.test[IO]()
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
        ).createTopic(subject, request, TopicDetails(1, 1, 1))
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
        kafkaAdmin <- KafkaAdminAlgebra.test[IO]()
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
        _ <- createTopicProgram.createTopic(subject, request, TopicDetails(1, 1, 1))
        _ <- metadata.addToMetadata(subject, request)
        metadataMap <- publishTo.get
        _ <- createTopicProgram.createTopic(subject, updatedRequest, TopicDetails(1, 1, 1))
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
        kafkaAdmin <- KafkaAdminAlgebra.test[IO]()
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
        ).createTopic(Subject.createValidated(subject).get, request, TopicDetails(1, 1, 1)).attempt
        topic <- kafkaAdmin.describeTopic(subject)
      } yield topic should not be defined).unsafeRunSync()
    }

    "not delete an existing topic when rolling back" in {
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      val subject = "dvs.subject"
      val metadataTopic = "dvs.test-metadata-topic"
      val topicDetails = TopicDetails(1, 1, 1)
      val request = createTopicMetadataRequest(keySchema, valueSchema)
      (for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafkaAdmin <- KafkaAdminAlgebra.test[IO]()
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
        kafkaAdmin <- KafkaAdminAlgebra.test[IO]()
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
        _ <- createTopicProgram.createTopic(subject, request, TopicDetails(1, 1, 1))
        _ <- metadata.addToMetadata(subject, request)
        metadataMap <- publishTo.get
        _ <- createTopicProgram.createTopic(subject, updatedRequest, TopicDetails(1, 1, 1))
        updatedMap <- publishTo.get
      } yield {
        val dd = metadataMap(metadataTopic)._2.get.get("deprecatedDate")
        val ud = updatedMap(metadataTopic)._2.get.get("deprecatedDate")
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
        kafkaAdmin <- KafkaAdminAlgebra.test[IO]()
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
        _ <- createTopicProgram.createTopic(subject, request, TopicDetails(1, 1, 1))
        _ <- metadata.addToMetadata(subject, request)
        metadataMap <- publishTo.get
        _ <- createTopicProgram.createTopic(subject, updatedRequest, TopicDetails(1, 1, 1))
        updatedMap <- publishTo.get
      } yield {
        val ud = metadataMap(metadataTopic)._2.get.get("deprecatedDate")
        val dd = updatedMap(metadataTopic)._2.get.get("deprecatedDate")
        ud shouldBe null
        Instant.parse(dd.toString) shouldBe a[Instant]
      }).unsafeRunSync()
    }

    "create topic with custom number of partitions" in {
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      val subject = "dvs.subject"
      (for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
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
          createTopicMetadataRequest(keySchema, valueSchema, numPartitions = refineMV[TopicMetadataV2Request.NumPartitionsPredicate](22).some),
          TopicDetails(1, 1, 1)
        )
        topic <- kafka.describeTopic(subject)
      } yield topic.get shouldBe Topic(subject, 22)).unsafeRunSync()
    }

    "throw error on topic with key and value field named same but with different type" in {

      val mismatchedValueSchema =
        SchemaBuilder
          .record("name")
          .fields()
          .name("isTrue")
          .`type`()
          .booleanType()
          .noDefault()
          .endRecord()

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      an [ValidationErrors] shouldBe thrownBy {(for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
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
          createTopicMetadataRequest(keySchema, mismatchedValueSchema),
          TopicDetails(1, 1, 1)
        )
      } yield fail("Should Fail to Create Topic - this yield should not be hit.")).unsafeRunSync()}
    }

    "throw error on topic with key that has field of type union [null, ...]" in {
      val union = SchemaBuilder.unionOf().nullType().and().stringType().endUnion()
      val recordWithNullDefault =
        SchemaBuilder
          .record("name")
          .fields()
          .name("nullableUnion")
          .`type`(union)
          .withDefault(null)
          .endRecord()

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      an [ValidationErrors] shouldBe thrownBy {(for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
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
          createTopicMetadataRequest(recordWithNullDefault, valueSchema),
          TopicDetails(1, 1, 1)
        )
      } yield fail("Should Fail to Create Topic - this yield should not be hit.")).unsafeRunSync()}
    }

    "do not throw error on topic with key that has field of type union [null, ...] if streamType is 'Event'" in {
      val union = SchemaBuilder.unionOf().nullType().and().stringType().endUnion()
      val recordWithNullDefault =
        SchemaBuilder
          .record("name")
          .fields()
          .name("nullableUnion")
          .`type`(union)
          .withDefault(null)
          .endRecord()

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      (for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
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
          createEventStreamTypeTopicMetadataRequest(recordWithNullDefault, valueSchema),
          TopicDetails(1, 1, 1)
        )
      } yield succeed).unsafeRunSync()
    }

    "throw error on topic with key that has field of type null" in {
      val recordWithNullType =
        SchemaBuilder
          .record("name")
          .fields()
          .name("nullableField")
          .`type`("null")
          .noDefault()
          .endRecord()

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      an [ValidationErrors] shouldBe thrownBy {(for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
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
          createTopicMetadataRequest(recordWithNullType, valueSchema),
          TopicDetails(1, 1, 1)
        )
      } yield fail("Should Fail to Create Topic - this yield should not be hit.")).unsafeRunSync()}
    }

    "do not throw error on topic with key that has field of type null if streamType is 'Event'" in {
      val recordWithNullType =
        SchemaBuilder
          .record("name")
          .fields()
          .name("nullableField")
          .`type`("null")
          .noDefault()
          .endRecord()

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      (for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
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
          createEventStreamTypeTopicMetadataRequest(recordWithNullType, valueSchema),
          TopicDetails(1, 1, 1)
        )
      } yield succeed).unsafeRunSync()
    }

    "throw error on schema evolution with illegal union logical type removal" in {
      val firstValue =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |        "name":"context",
          |        "type":[
          |                 {
          |                   "type": "string",
          |                   "logicalType": "uuid"
          |                 },
          |                 "null"
          |               ]
          |     },
          |     {
          |        "name":"context2",
          |        "type":[
          |                 {
          |                   "type": "string",
          |                   "logicalType": "uuid"
          |                 },
          |                 "null"
          |               ]
          |     }
          |  ]
          |}
  """.stripMargin
      val valueEvolution =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "context",
          |       "type": ["string", "null" ]
          |     },
          |     {
          |       "name": "context2",
          |       "type": ["string", "null" ]
          |     }
          |  ]
          |}
  """.stripMargin
      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      an [ValidationErrors] shouldBe thrownBy {(for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
        kafkaClient <- KafkaClientAlgebra.test[IO]
        metadata <- metadataAlgebraF("dvs.test-metadata-topic", schemaRegistry, kafkaClient)
        tcp = new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
          policy,
          Subject.createValidated("dvs.test-metadata-topic").get,
          metadata
        )
        _ <- tcp.createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchema, firstValueSchema),
          TopicDetails(1, 1, 1)
        )
        _ <- tcp.createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchema, valueSchemaEvolution),
          TopicDetails(1, 1, 1)
        )
      } yield  fail("Should Fail to Create Topic - this yield should not be hit.")).unsafeRunSync()}}

    "throw error on schema evolution with illegal union logical type addition" in {
      val firstValue =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "context",
          |       "type": ["string", "null" ]
          |     },
          |     {
          |       "name": "context2",
          |       "type": ["string", "null" ]
          |     }
          |  ]
          |}
        """.stripMargin
      val valueEvolution =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |        "name":"context",
          |        "type":[
          |                 {
          |                   "type": "string",
          |                   "logicalType": "uuid"
          |                 },
          |                 "null"
          |               ]
          |     },
          |     {
          |        "name":"context2",
          |        "type":[
          |                 {
          |                   "type": "string",
          |                   "logicalType": "uuid"
          |                 },
          |                 "null"
          |               ]
          |     }
          |  ]
          |}
        """.stripMargin

      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      an [ValidationErrors] shouldBe thrownBy {(for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
        kafkaClient <- KafkaClientAlgebra.test[IO]
        metadata <- metadataAlgebraF("dvs.test-metadata-topic", schemaRegistry, kafkaClient)
        tcp = new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
          policy,
          Subject.createValidated("dvs.test-metadata-topic").get,
          metadata
        )
        _ <- tcp.createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchema, firstValueSchema),
          TopicDetails(1, 1, 1)
        )
        _ <- tcp.createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchema, valueSchemaEvolution),
          TopicDetails(1, 1, 1)
        )
      } yield  fail("Should Fail to Create Topic - this yield should not be hit.")).unsafeRunSync()}}

    "throw error on schema evolution with illegal union logical type change" in {
      val firstValue =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |        "name":"context",
          |        "type":[
          |                 {
          |                   "type": "string",
          |                   "logicalType": "uuid"
          |                 },
          |                 "null"
          |               ]
          |     },
          |     {
          |        "name":"context2",
          |        "type":[
          |                 {
          |                   "type": "string",
          |                   "logicalType": "uuid"
          |                 },
          |                 "null"
          |               ]
          |     }
          |  ]
          |}
        """.stripMargin
      val valueEvolution =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |        "name":"context",
          |        "type":[
          |                 {
          |                   "type": "string",
          |                   "logicalType": "date"
          |                 },
          |                 "null"
          |               ]
          |     },
          |     {
          |        "name":"context2",
          |        "type":[
          |                 {
          |                   "type": "string",
          |                   "logicalType": "date"
          |                 },
          |                 "null"
          |               ]
          |     }
          |  ]
          |}
        """.stripMargin

      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      an [ValidationErrors] shouldBe thrownBy {(for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
        kafkaClient <- KafkaClientAlgebra.test[IO]
        metadata <- metadataAlgebraF("dvs.test-metadata-topic", schemaRegistry, kafkaClient)
        tcp = new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
          policy,
          Subject.createValidated("dvs.test-metadata-topic").get,
          metadata
        )
        _ <- tcp.createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchema, firstValueSchema),
          TopicDetails(1, 1, 1)
        )
        _ <- tcp.createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchema, valueSchemaEvolution),
          TopicDetails(1, 1, 1)
        )
      } yield  fail("Should Fail to Create Topic - this yield should not be hit.")).unsafeRunSync()}}

    "throw error on schema evolution with illegal key field logical type change string" in {
      val firstKey =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "fields": [
          |    {
          |      "name": "keyThing",
          |      "type":{
          |        "type": "string",
          |        "logicalType":"uuid"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin
      val keyEvolution =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "fields": [
          |    {
          |      "name": "keyThing",
          |      "type":{
          |        "type": "string",
          |        "logicalType":"date"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin
      val firstKeySchema = new Schema.Parser().parse(firstKey)
      val keySchemaEvolution = new Schema.Parser().parse(keyEvolution)

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      an [ValidationErrors] shouldBe thrownBy {(for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
        kafkaClient <- KafkaClientAlgebra.test[IO]
        metadata <- metadataAlgebraF("dvs.test-metadata-topic", schemaRegistry, kafkaClient)
        tcp = new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
          policy,
          Subject.createValidated("dvs.test-metadata-topic").get,
          metadata
        )
        _ <- tcp.createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(firstKeySchema, valueSchema),
          TopicDetails(1, 1, 1)
        )
        _ <- tcp.createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchemaEvolution, valueSchema),
          TopicDetails(1, 1, 1)
        )
      } yield  fail("Should Fail to Create Topic - this yield should not be hit.")).unsafeRunSync()}}

    "throw error on schema evolution with illegal value field logical type removal" in {
      val firstValue =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "fields": [
          |    {
          |      "name": "valueThing",
          |      "type":{
          |        "type": "string",
          |        "logicalType":"uuid"
          |      }
          |    }
          |  ]
          |}
    """.stripMargin
      val valueEvolution =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "fields": [
          |    {
          |      "name": "valueThing",
          |      "type": "string"
          |    }
          |  ]
          |}
    """.stripMargin
      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      an [ValidationErrors] shouldBe thrownBy {(for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
        kafkaClient <- KafkaClientAlgebra.test[IO]
        metadata <- metadataAlgebraF("dvs.test-metadata-topic", schemaRegistry, kafkaClient)
        tcp = new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
          policy,
          Subject.createValidated("dvs.test-metadata-topic").get,
          metadata
        )
        _ <- tcp.createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchema, firstValueSchema),
          TopicDetails(1, 1, 1)
        )
        _ <- tcp.createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchema, valueSchemaEvolution),
          TopicDetails(1, 1, 1)
        )
      } yield  fail("Should Fail to Create Topic - this yield should not be hit.")).unsafeRunSync()}}

    "throw error on schema evolution with illegal value array with field logical type removal" in {
      val firstValue =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "ArrayOfThings",
          |       "type": {
          |         "type": "array",
          |         "items":
          |           {
          |             "logicalType": "date",
          |             "type": "int"
          |           }
          |       }
          |     }
          |  ]
          |}
  """.stripMargin
      val valueEvolution =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "ArrayOfThings",
          |       "type": {
          |         "type": "array",
          |         "items":
          |           {
          |             "type": "int"
          |           }
          |       }
          |     }
          |  ]
          |}
  """.stripMargin
      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      an [ValidationErrors] shouldBe thrownBy {(for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
        kafkaClient <- KafkaClientAlgebra.test[IO]
        metadata <- metadataAlgebraF("dvs.test-metadata-topic", schemaRegistry, kafkaClient)
        tcp = new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
          policy,
          Subject.createValidated("dvs.test-metadata-topic").get,
          metadata
        )
        _ <- tcp.createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchema, firstValueSchema),
          TopicDetails(1, 1, 1)
        )
        _ <- tcp.createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchema, valueSchemaEvolution),
          TopicDetails(1, 1, 1)
        )
      } yield  fail("Should Fail to Create Topic - this yield should not be hit.")).unsafeRunSync()}}

    "throw error on schema evolution with illegal value map with field logical type removal" in {
      val firstValue =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "ArrayOfThings",
          |       "type": {
          |         "type": "map",
          |         "values":
          |           {
          |             "logicalType": "date",
          |             "type": "int"
          |           }
          |       }
          |     }
          |  ]
          |}
  """.stripMargin
      val valueEvolution =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "ArrayOfThings",
          |       "type": {
          |         "type": "map",
          |         "values":
          |           {
          |             "type": "int"
          |           }
          |       }
          |     }
          |  ]
          |}
  """.stripMargin
      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      an [ValidationErrors] shouldBe thrownBy {(for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
        kafkaClient <- KafkaClientAlgebra.test[IO]
        metadata <- metadataAlgebraF("dvs.test-metadata-topic", schemaRegistry, kafkaClient)
        tcp = new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
          policy,
          Subject.createValidated("dvs.test-metadata-topic").get,
          metadata
        )
        _ <- tcp.createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchema, firstValueSchema),
          TopicDetails(1, 1, 1)
        )
        _ <- tcp.createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchema, valueSchemaEvolution),
          TopicDetails(1, 1, 1)
        )
      } yield  fail("Should Fail to Create Topic - this yield should not be hit.")).unsafeRunSync()}}


    "do not throw logical type validation error on schema evolution with no change, array" in {
      val firstValue =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "ArrayOfThings",
          |       "type": {
          |         "type": "array",
          |         "items":
          |           {
          |             "logicalType": "date",
          |             "type": "int"
          |           }
          |       }
          |     }
          |  ]
          |}
  """.stripMargin
      val valueEvolution =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "ArrayOfThings",
          |       "type": {
          |         "type": "array",
          |         "items":
          |           {
          |             "logicalType": "date",
          |             "type": "int"
          |           }
          |       }
          |     }
          |  ]
          |}
  """.stripMargin
      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      (for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
        kafkaClient <- KafkaClientAlgebra.test[IO]
        metadata <- metadataAlgebraF("dvs.test-metadata-topic", schemaRegistry, kafkaClient)
        tcp = new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
          policy,
          Subject.createValidated("dvs.test-metadata-topic").get,
          metadata
        )
        _ <- tcp.createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchema, firstValueSchema),
          TopicDetails(1, 1, 1)
        )
        _ <- tcp.createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchema, valueSchemaEvolution),
          TopicDetails(1, 1, 1)
        )
      } yield succeed).unsafeRunSync()}

    "do not throw logical type validation error on schema evolution with no change, map" in {
      val firstValue =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "ArrayOfThings",
          |       "type": {
          |         "type": "map",
          |         "values":
          |           {
          |             "logicalType": "date",
          |             "type": "int"
          |           }
          |       }
          |     }
          |  ]
          |}
  """.stripMargin
      val valueEvolution =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "ArrayOfThings",
          |       "type": {
          |         "type": "map",
          |         "values":
          |           {
          |             "logicalType": "date",
          |             "type": "int"
          |           }
          |       }
          |     }
          |  ]
          |}
  """.stripMargin
      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      (for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
        kafkaClient <- KafkaClientAlgebra.test[IO]
        metadata <- metadataAlgebraF("dvs.test-metadata-topic", schemaRegistry, kafkaClient)
        tcp = new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
          policy,
          Subject.createValidated("dvs.test-metadata-topic").get,
          metadata
        )
        _ <- tcp.createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchema, firstValueSchema),
          TopicDetails(1, 1, 1)
        )
        _ <- tcp.createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchema, valueSchemaEvolution),
          TopicDetails(1, 1, 1)
        )
      } yield succeed).unsafeRunSync()}

    "do not throw logical type validation error on schema evolution with no change, nested record" in {
      val firstValue =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "RecordOfThings",
          |       "type": {
          |         "type": "record",
          |         "name": "NestedRecord",
          |         "fields": [
          |           {
          |             "name": "address",
          |             "type": "string"
          |           }
          |         ]
          |       }
          |     }
          |  ]
          |}
  """.stripMargin
      val valueEvolution =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "RecordOfThings",
          |       "type": {
          |         "type": "record",
          |         "name": "NestedRecord",
          |         "fields": [
          |           {
          |             "name": "address",
          |             "type": "string"
          |           }
          |         ]
          |       }
          |     }
          |  ]
          |}
  """.stripMargin
      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      (for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
        kafkaClient <- KafkaClientAlgebra.test[IO]
        metadata <- metadataAlgebraF("dvs.test-metadata-topic", schemaRegistry, kafkaClient)
        tcp = new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
          policy,
          Subject.createValidated("dvs.test-metadata-topic").get,
          metadata
        )
        _ <- tcp.createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchema, firstValueSchema),
          TopicDetails(1, 1, 1)
        )
        _ <- tcp.createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchema, valueSchemaEvolution),
          TopicDetails(1, 1, 1)
        )
      } yield succeed).unsafeRunSync()}

    "throw error on schema evolution with illegal key field logical type change within a nested record" in {
      val firstKey =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "RecordOfThings",
          |       "type": {
          |         "type": "record",
          |         "name": "NestedRecord",
          |         "fields": [
          |           {
          |             "name": "address",
          |             "type": {
          |               "type": "string",
          |               "logicalType": "uuid"
          |             }
          |           }
          |         ]
          |       }
          |     }
          |  ]
          |}
      """.stripMargin
      val keyEvolution =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "RecordOfThings",
          |       "type": {
          |         "type": "record",
          |         "name": "NestedRecord",
          |         "fields": [
          |           {
          |             "name": "address",
          |             "type": "string"
          |           }
          |         ]
          |       }
          |     }
          |  ]
          |}
      """.stripMargin
      val firstKeySchema = new Schema.Parser().parse(firstKey)
      val keySchemaEvolution = new Schema.Parser().parse(keyEvolution)

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      an [ValidationErrors] shouldBe thrownBy {(for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
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
          createTopicMetadataRequest(firstKeySchema, valueSchema),
          TopicDetails(1, 1, 1)
        )
        _ <- new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
          policy,
          Subject.createValidated("dvs.test-metadata-topic").get,
          metadata
        ).createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchemaEvolution, valueSchema),
          TopicDetails(1, 1, 1)
        )
      } yield fail("Should Fail to Create Topic - this yield should not be hit.")).unsafeRunSync()}}


    "throw error on schema evolution with illegal key field logical type change" in {
      val firstKey =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "fields": [
          |    {
          |      "name": "keyThing",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin
      val keyEvolution =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "fields": [
          |    {
          |      "name": "keyThing",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-micros"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin
      val firstKeySchema = new Schema.Parser().parse(firstKey)
      val keySchemaEvolution = new Schema.Parser().parse(keyEvolution)

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      an [ValidationErrors] shouldBe thrownBy {(for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
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
          createTopicMetadataRequest(firstKeySchema, valueSchema),
          TopicDetails(1, 1, 1)
        )
        _ <- new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
          policy,
          Subject.createValidated("dvs.test-metadata-topic").get,
          metadata
        ).createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchemaEvolution, valueSchema),
          TopicDetails(1, 1, 1)
        )
      } yield fail("Should Fail to Create Topic - this yield should not be hit.")).unsafeRunSync()}}

    "throw error on schema evolution with illegal value field logical type change" in {
      val firstValue =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "fields": [
          |    {
          |      "name": "valueThing",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin
      val valueEvolution =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "fields": [
          |    {
          |      "name": "valueThing",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-micros"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin
      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      an [ValidationErrors] shouldBe thrownBy {(for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
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
          createTopicMetadataRequest(keySchema, firstValueSchema),
          TopicDetails(1, 1, 1)
        )
        _ <- new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
          policy,
          Subject.createValidated("dvs.test-metadata-topic").get,
          metadata
        ).createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchema, valueSchemaEvolution),
          TopicDetails(1, 1, 1)
        )
      } yield fail("Should Fail to Create Topic - this yield should not be hit.")).unsafeRunSync()}}

    "throw error on schema evolution with illegal key field logical type addition" in {
      val firstKey =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "namespace": "dvs.data_platform.dvs_sandbox",
          |  "fields": [
          |    {
          |      "name": "valueThing",
          |      "type": "string"
          |    }
          |  ]
          |}
      """.stripMargin
      val keyEvolution =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "namespace": "dvs.data_platform.dvs_sandbox",
          |  "fields": [
          |    {
          |      "name": "valueThing",
          |      "type":{
          |        "type": "string",
          |        "logicalType":"uuid"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin
      val firstKeySchema = new Schema.Parser().parse(firstKey)
      val keySchemaEvolution = new Schema.Parser().parse(keyEvolution)

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      an [ValidationErrors] shouldBe thrownBy {(for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
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
          createTopicMetadataRequest(firstKeySchema, valueSchema),
          TopicDetails(1, 1, 1)
        )
        _ <- new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
          policy,
          Subject.createValidated("dvs.test-metadata-topic").get,
          metadata
        ).createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchemaEvolution, valueSchema),
          TopicDetails(1, 1, 1)
        )
      } yield fail("Should Fail to Create Topic - this yield should not be hit.")).unsafeRunSync()}}

    "throw error on schema evolution with illegal value field logical type addition" in {
      val firstValue =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "namespace": "dvs.data_platform.dvs_sandbox",
          |  "fields": [
          |    {
          |      "name": "valueThing",
          |      "type": "string"
          |    }
          |  ]
          |}
      """.stripMargin
      val valueEvolution =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "namespace": "dvs.data_platform.dvs_sandbox",
          |  "fields": [
          |    {
          |      "name": "valueThing",
          |      "type":{
          |        "type": "string",
          |        "logicalType":"uuid"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin
      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      an [ValidationErrors] shouldBe thrownBy {(for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
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
          createTopicMetadataRequest(keySchema, firstValueSchema),
          TopicDetails(1, 1, 1)
        )
        _ <- new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
          policy,
          Subject.createValidated("dvs.test-metadata-topic").get,
          metadata
        ).createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchema, valueSchemaEvolution),
          TopicDetails(1, 1, 1)
        )
      } yield fail("Should Fail to Create Topic - this yield should not be hit.")).unsafeRunSync()}}

    "do not throw error on legal schema evolution with enum" in {
      val firstValue =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "namespace": "dvs.data_platform.dvs_sandbox",
          |  "fields": [
          |    {
          |			"name": "testEnum",
          |			"type": {
          |            "type": "enum",
          |            "name": "test_type",
          |            "symbols": ["test1", "test2"]
          |        }
          |		}
          |  ]
          |}
      """.stripMargin
      val valueEvolution =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "namespace": "dvs.data_platform.dvs_sandbox",
          |  "fields": [
          |    {
          |			"name": "testEnum",
          |			"type": {
          |            "type": "enum",
          |            "name": "test_type",
          |            "symbols": ["test1", "test2"]
          |        }
          |		}
          |  ]
          |}
      """.stripMargin
      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      (for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
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
          createTopicMetadataRequest(keySchema, firstValueSchema),
          TopicDetails(1, 1, 1)
        )
        _ <- new CreateTopicProgram[IO](
          schemaRegistry,
          kafka,
          kafkaClient,
          policy,
          Subject.createValidated("dvs.test-metadata-topic").get,
          metadata
        ).createTopic(
          Subject.createValidated("dvs.subject").get,
          createTopicMetadataRequest(keySchema, valueSchemaEvolution),
          TopicDetails(1, 1, 1)
        )
      } yield succeed).unsafeRunSync()}


    "successfully validate topic schema with value that has field of type union [null, ...]" in {
      val union = SchemaBuilder.unionOf().nullType().and().stringType().endUnion()
      val recordWithNullDefault =
        SchemaBuilder
          .record("name")
          .fields()
          .name("nullableUnion")
          .`type`(union)
          .withDefault(null)
          .endRecord()

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      val subject = Subject.createValidated("dvs.subject").get
      val topicMetadataV2Request = createTopicMetadataRequest(keySchema, recordWithNullDefault)
      val resource: Resource[IO, Assertion] = (for {
        schemaRegistry <- Resource.liftF(SchemaRegistry.test[IO])
        kafka <- Resource.liftF(KafkaAdminAlgebra.test[IO]())
        kafkaClient <- Resource.liftF(KafkaClientAlgebra.test[IO])
        metadata <- Resource.liftF(metadataAlgebraF("dvs.test-metadata-topic", schemaRegistry, kafkaClient))
        ctProgram = new CreateTopicProgram[IO](schemaRegistry, kafka, kafkaClient, policy, Subject.createValidated("dvs.test-metadata-topic").get, metadata)
        _ <- ctProgram.registerSchemas(subject ,keySchema, recordWithNullDefault)
        _ <- ctProgram.createTopicResource(subject, TopicDetails(1,1,1))
        _ <- Resource.liftF(ctProgram.createTopicFromMetadataOnly(subject, topicMetadataV2Request))
      } yield (succeed))
      resource.use(_ => Bracket[IO, Throwable].unit).unsafeRunSync()
    }

    "succesfully validate topic schema with value that has field of type null" in {
      val recordWithNullType =
        SchemaBuilder
          .record("name")
          .fields()
          .name("nullableField")
          .`type`("null")
          .noDefault()
          .endRecord()

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      val subject = Subject.createValidated("dvs.subject").get
      val topicMetadataV2Request = createTopicMetadataRequest(keySchema, recordWithNullType)
      val resource: Resource[IO, Assertion] = (for {
        schemaRegistry <- Resource.liftF(SchemaRegistry.test[IO])
        kafka <- Resource.liftF(KafkaAdminAlgebra.test[IO]())
        kafkaClient <- Resource.liftF(KafkaClientAlgebra.test[IO])
        metadata <- Resource.liftF(metadataAlgebraF("dvs.test-metadata-topic", schemaRegistry, kafkaClient))
        ctProgram = new CreateTopicProgram[IO](schemaRegistry, kafka, kafkaClient, policy, Subject.createValidated("dvs.test-metadata-topic").get, metadata)
        _ <- ctProgram.registerSchemas(subject ,keySchema, recordWithNullType)
        _ <- ctProgram.createTopicResource(subject, TopicDetails(1,1,1))
        _ <- Resource.liftF(ctProgram.createTopicFromMetadataOnly(subject, topicMetadataV2Request))
      } yield (succeed))
      resource.use(_ => Bracket[IO, Throwable].unit).unsafeRunSync()
    }

    "succesfully validate topic schema with key that has field of type union [not null, not null]" in {
      val union = SchemaBuilder.unionOf().intType().and().stringType().endUnion()
      val recordWithNullDefault =
        SchemaBuilder
          .record("name")
          .fields()
          .name("nullableUnion")
          .`type`(union)
          .withDefault(5)
          .endRecord()

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      val subject = Subject.createValidated("dvs.subject").get
      val topicMetadataV2Request = createTopicMetadataRequest(recordWithNullDefault, valueSchema)
      val resource: Resource[IO, Assertion] = (for {
        schemaRegistry <- Resource.liftF(SchemaRegistry.test[IO])
        kafka <- Resource.liftF(KafkaAdminAlgebra.test[IO]())
        kafkaClient <- Resource.liftF(KafkaClientAlgebra.test[IO])
        metadata <- Resource.liftF(metadataAlgebraF("dvs.test-metadata-topic", schemaRegistry, kafkaClient))
        ctProgram = new CreateTopicProgram[IO](schemaRegistry, kafka, kafkaClient, policy, Subject.createValidated("dvs.test-metadata-topic").get, metadata)
        _ <- ctProgram.registerSchemas(subject, recordWithNullDefault, valueSchema)
        _ <- ctProgram.createTopicResource(subject, TopicDetails(1,1,1))
        _ <- Resource.liftF(ctProgram.createTopicFromMetadataOnly(subject, topicMetadataV2Request))
      } yield (succeed))
      resource.use(_ => Bracket[IO, Throwable].unit).unsafeRunSync()
    }

    /*"successfully evolve schema which had mismatched types in past version" in {
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      val subject = Subject.createValidated("dvs.subject").get
      val mismatchedValueSchema = SchemaBuilder.record("name").fields().name("isTrue").`type`().booleanType().noDefault().endRecord()
      val mismatchedValueSchemaEvolution = SchemaBuilder.record("name").fields().name("isTrue").`type`().booleanType().noDefault().nullableInt("nullInt", 12).endRecord()
      val topicMetadataV2Request = createTopicMetadataRequest(keySchema, mismatchedValueSchema)
      val resource: Resource[IO, Assertion] = (for {
        schemaRegistry <- Resource.liftF(SchemaRegistry.test[IO])
        kafka <- Resource.liftF(KafkaAdminAlgebra.test[IO]())
        kafkaClient <- Resource.liftF(KafkaClientAlgebra.test[IO])
        metadata <- Resource.liftF(metadataAlgebraF("dvs.test-metadata-topic", schemaRegistry, kafkaClient))
        ctProgram = new CreateTopicProgram[IO](schemaRegistry, kafka, kafkaClient, policy, Subject.createValidated("dvs.test-metadata-topic").get, metadata)
        _ <- ctProgram.registerSchemas(subject ,keySchema, mismatchedValueSchema)
        _ <- ctProgram.createTopicResource(subject, TopicDetails(1,1,1))
        _ <- Resource.liftF(ctProgram.createTopicFromMetadataOnly(subject, topicMetadataV2Request))
        _ <- Resource.liftF(ctProgram.createTopic(
          subject,
          createTopicMetadataRequest(keySchema, mismatchedValueSchemaEvolution),
          TopicDetails(1, 1, 1)
        ))
      } yield (succeed))
      resource.use(_ => Bracket[IO, Throwable].unit).unsafeRunSync()
    }*/

    "throw error if key schema is not registered as record type before creating topic from metadata only" in {
      val incorrectKeySchema = new Schema.Parser().parse("""
                                                           |{
                                                           |	"type": "string"
                                                           |}""".stripMargin)
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      val subject = Subject.createValidated("dvs.subject").get
      val topicMetadataV2Request = createTopicMetadataRequest(incorrectKeySchema, valueSchema)
      val resource: Resource[IO, Assertion] = (for {
        schemaRegistry <- Resource.liftF(SchemaRegistry.test[IO])
        kafka <- Resource.liftF(KafkaAdminAlgebra.test[IO]())
        kafkaClient <- Resource.liftF(KafkaClientAlgebra.test[IO])
        metadata <- Resource.liftF(metadataAlgebraF("dvs.test-metadata-topic", schemaRegistry, kafkaClient))
        ctProgram = new CreateTopicProgram[IO](schemaRegistry, kafka, kafkaClient, policy, Subject.createValidated("dvs.test-metadata-topic").get, metadata)
        _ <- ctProgram.registerSchemas(subject ,incorrectKeySchema, valueSchema)
        _ <- ctProgram.createTopicResource(subject, TopicDetails(1,1,1))
        _ <- Resource.liftF(ctProgram.createTopicFromMetadataOnly(subject, topicMetadataV2Request))
      } yield fail("Should Fail to add Metadata - this yield should not be hit."))}

    "throw error if value schema is not registered as record type before creating topic from metadata only" in {
      val incorrectValueSchema = new Schema.Parser().parse("""
                                                             |{
                                                             |	"type": "string"
                                                             |}""".stripMargin)
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      val subject = Subject.createValidated("dvs.subject").get
      val topicMetadataV2Request = createTopicMetadataRequest(keySchema, incorrectValueSchema)
      val resource: Resource[IO, Assertion] = (for {
        schemaRegistry <- Resource.liftF(SchemaRegistry.test[IO])
        kafka <- Resource.liftF(KafkaAdminAlgebra.test[IO]())
        kafkaClient <- Resource.liftF(KafkaClientAlgebra.test[IO])
        metadata <- Resource.liftF(metadataAlgebraF("dvs.test-metadata-topic", schemaRegistry, kafkaClient))
        ctProgram = new CreateTopicProgram[IO](schemaRegistry, kafka, kafkaClient, policy, Subject.createValidated("dvs.test-metadata-topic").get, metadata)
        _ <- ctProgram.registerSchemas(subject ,keySchema, incorrectValueSchema)
        _ <- ctProgram.createTopicResource(subject, TopicDetails(1,1,1))
        _ <- Resource.liftF(ctProgram.createTopicFromMetadataOnly(subject, topicMetadataV2Request))
      } yield fail("Should Fail to add Metadata - this yield should not be hit."))}

    "successfully creating topic from metadata only where key and value schemas are records" in {
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      val subject = Subject.createValidated("dvs.subject").get
      val topicMetadataV2Request = createTopicMetadataRequest(keySchema, valueSchema)
      val resource: Resource[IO, Assertion] = (for {
        schemaRegistry <- Resource.liftF(SchemaRegistry.test[IO])
        kafka <- Resource.liftF(KafkaAdminAlgebra.test[IO]())
        kafkaClient <- Resource.liftF(KafkaClientAlgebra.test[IO])
        metadata <- Resource.liftF(metadataAlgebraF("dvs.test-metadata-topic", schemaRegistry, kafkaClient))
        ctProgram = new CreateTopicProgram[IO](schemaRegistry, kafka, kafkaClient, policy, Subject.createValidated("dvs.test-metadata-topic").get, metadata)
        _ <- ctProgram.registerSchemas(subject ,keySchema, valueSchema)
        _ <- ctProgram.createTopicResource(subject, TopicDetails(1,1,1))
        _ <- Resource.liftF(ctProgram.createTopicFromMetadataOnly(subject, topicMetadataV2Request))
      } yield (succeed))
      resource.use(_ => Bracket[IO, Throwable].unit).unsafeRunSync()
    }

    "throw error of schema nullable values don't have default value" in {
      val union = SchemaBuilder.unionOf().nullType().and().stringType().endUnion()

      val nullableValue = SchemaBuilder
                          .record("val")
                          .fields()
                          .name("itsnullable")
                          .`type`(union)
                          .noDefault()
                          .endRecord()

      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      an [ValidationErrors] shouldBe thrownBy {(for {
        schemaRegistry <- SchemaRegistry.test[IO]
        kafka <- KafkaAdminAlgebra.test[IO]()
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
          createTopicMetadataRequest(keySchema, nullableValue),
          TopicDetails(1, 1, 1)
        )
      } yield fail("Should Fail to Create Topic - this yield should not be hit.")).unsafeRunSync()}
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

    override def consumeMessagesWithOffsetInfo(topicName: TopicName, consumerGroup: ConsumerGroup, commitOffsets: Boolean): fs2.Stream[IO, ((GenericRecord, Option[GenericRecord], Option[Headers]), (Partition, hydra.kafka.algebras.KafkaClientAlgebra.Offset))] = fs2.Stream.empty

    override def consumeStringKeyMessagesWithOffsetInfo(topicName: TopicName, consumerGroup: ConsumerGroup, commitOffsets: Boolean): fs2.Stream[IO, ((Option[String], Option[GenericRecord], Option[Headers]), (Partition, hydra.kafka.algebras.KafkaClientAlgebra.Offset))] = fs2.Stream.empty

    override def streamStringKeyFromGivenPartitionAndOffset(topicName: TopicName, consumerGroup: ConsumerGroup, commitOffsets: Boolean, topicAndPartition: List[(TopicPartition, Offset)]): fs2.Stream[IO, ((Option[String], Option[GenericRecord], Option[Headers]), (Partition, Offset), Timestamp)] = ???

    override def streamAvroKeyFromGivenPartitionAndOffset(topicName: TopicName, consumerGroup: ConsumerGroup, commitOffsets: Boolean, topicAndPartition: List[(TopicPartition, Offset)]): fs2.Stream[IO, ((GenericRecord, Option[GenericRecord], Option[Headers]), (Partition, Offset), Timestamp)] = ???
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