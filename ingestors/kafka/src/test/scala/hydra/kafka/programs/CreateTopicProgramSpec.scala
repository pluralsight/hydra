package hydra.kafka.programs

import cats.effect._
import cats.syntax.all._

import java.time.Instant
import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import fs2.kafka.Headers
import fs2.kafka._
import hydra.avro.registry.SchemaRegistry
import hydra.avro.registry.SchemaRegistry.{SchemaId, SchemaVersion}
import hydra.kafka.algebras.KafkaAdminAlgebra.{Topic, TopicName}
import hydra.kafka.algebras.KafkaClientAlgebra.{ConsumerGroup, Offset, Partition, PublishError, PublishResponse}
import hydra.kafka.algebras.MetadataAlgebra.TopicMetadataContainer
import hydra.kafka.algebras.{KafkaAdminAlgebra, KafkaClientAlgebra, MetadataAlgebra, TestMetadataAlgebra}
import hydra.kafka.model.ContactMethod.Email
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model._
import hydra.kafka.util.KafkaUtils.TopicDetails
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.scalatest.matchers.should.Matchers
import retry.{RetryPolicies, RetryPolicy}
import eu.timepit.refined._
import hydra.common.NotificationsTestSuite
import hydra.common.alerting.sender.InternalNotificationSender
import hydra.common.util.InstantUtils.dateStringToInstant
import hydra.common.validation.ValidationError.ValidationCombinedErrors
import hydra.kafka.IOSuite
import hydra.kafka.algebras.RetryableFs2Stream.RetryPolicy.Once

import scala.concurrent.ExecutionContext
import hydra.kafka.model.TopicMetadataV2Request.NumPartitions
import hydra.kafka.programs.CreateTopicProgram.MetadataOnlyTopicDoesNotExist
import hydra.kafka.programs.TopicSchemaError._
import hydra.kafka.utils.TopicUtils
import org.apache.avro.SchemaBuilder.{FieldAssembler, GenericDefault}
import org.apache.kafka.common.TopicPartition
import org.scalatest.freespec.AsyncFreeSpec

import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter
import scala.language.implicitConversions

class CreateTopicProgramSpec extends AsyncFreeSpec with Matchers with IOSuite {
  import CreateTopicProgramSpec._

  "CreateTopicSpec" - {
    "register the two avro schemas" in {
      for {
        ts          <- initTestServices()
        _           <- ts.program.createTopic(subject, topicMetadataRequest, topicDetails, true)
        allSubjects <- ts.schemaRegistry.getAllSubjects
      } yield allSubjects.size shouldBe 2
    }

    "rollback schema creation on error" in {
      case class TestState(deleteSchemaWasCalled: Boolean, numSchemasRegistered: Int)

      def getSchemaRegistry(ref: Ref[IO, TestState]): SchemaRegistry[IO] =
        new SchemaRegistry[IO] {
          override def registerSchema(subject: String, schema: Schema): IO[SchemaId] = ref.get.flatMap {
            case TestState(_, 1) => IO.raiseError(new Exception("Something horrible went wrong!"))
            case t: TestState =>
              val schemaId = t.numSchemasRegistered + 1
              ref.set(t.copy(numSchemasRegistered = schemaId)) *> IO.pure(schemaId)
          }

          override def deleteSchemaOfVersion(subject: String, version: SchemaVersion): IO[Unit] =
            ref.update(_.copy(deleteSchemaWasCalled = true))

          override def getVersion(subject: String, schema: Schema): IO[SchemaVersion] =
            ref.get.map(testState => testState.numSchemasRegistered + 1)

          override def getAllVersions(subject: String): IO[List[Int]] = IO.pure(List())
          override def getAllSubjects: IO[List[String]] = IO.pure(List())
          override def getSchemaRegistryClient: IO[SchemaRegistryClient] =
            IO.raiseError(new Exception("Something horrible went wrong!"))

          override def getLatestSchemaBySubject(subject: String): IO[Option[Schema]] = IO.pure(None)
          override def getSchemaFor(subject: String, schemaVersion: SchemaVersion): IO[Option[Schema]] = IO.pure(None)
          override def deleteSchemaSubject(subject: String): IO[Unit] = IO.pure(())
        }
      for {
        ref    <- Ref[IO].of(TestState(deleteSchemaWasCalled = false, 0))
        ts     <- initTestServices(schemaRegistry = getSchemaRegistry(ref).some)
        _      <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, valueSchema), topicDetails, true).attempt
        result <- ref.get
      } yield result.deleteSchemaWasCalled shouldBe true
    }

    "retry given number of attempts" in {
      val numberRetries = 3

      def getSchemaRegistry(ref: Ref[IO, Int]): SchemaRegistry[IO] =
        new SchemaRegistry[IO] {
          override def registerSchema(subject: String, schema: Schema): IO[SchemaId] =
            ref.get.flatMap(n => ref.set(n + 1) *> IO.raiseError(new Exception("Something horrible went wrong!")))

          override def deleteSchemaOfVersion(subject: String, version: SchemaVersion): IO[Unit] = IO.unit
          override def getVersion(subject: String, schema: Schema): IO[SchemaVersion] = IO.pure(1)
          override def getAllVersions(subject: String): IO[List[Int]] = IO.pure(Nil)
          override def getAllSubjects: IO[List[String]] = IO.pure(Nil)
          override def getSchemaRegistryClient: IO[SchemaRegistryClient] = IO.raiseError(new Exception("Something horrible went wrong!"))
          override def getLatestSchemaBySubject(subject: String): IO[Option[Schema]] = IO.pure(None)
          override def getSchemaFor(subject: String, schemaVersion: SchemaVersion): IO[Option[Schema]] = IO.pure(None)
          override def deleteSchemaSubject(subject: String): IO[Unit] = IO.pure(())
        }

      for {
        ref    <- Ref[IO].of(0)
        ts     <- initTestServices(schemaRegistry = getSchemaRegistry(ref).some, retryPolicy = RetryPolicies.limitRetries(numberRetries))
        _      <-ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, valueSchema), topicDetails).attempt
        result <- ref.get
      } yield result shouldBe numberRetries + 1
    }

    "not remove existing schemas on rollback" in {
      type SchemaName = String
      case class TestState(schemas: Map[SchemaName, SchemaVersion])

      def getSchemaRegistry(ref: Ref[IO, TestState]): SchemaRegistry[IO] =
        new SchemaRegistry[IO] {
          override def registerSchema(subject: String, schema: Schema): IO[SchemaId] =
            ref.get.flatMap { ts =>
              if (subject.contains("-value")) IO.raiseError(new Exception) else IO.pure(ts.schemas(subject))
            }

          override def deleteSchemaOfVersion(subject: String, version: SchemaVersion): IO[Unit] =
            ref.update(ts => ts.copy(schemas = ts.schemas - subject))

          override def getVersion(subject: String, schema: Schema): IO[SchemaVersion] = ref.get.map(_.schemas(subject))
          override def getAllVersions(subject: String): IO[List[Int]] = IO.pure(Nil)
          override def getAllSubjects: IO[List[String]] = IO.pure(Nil)
          override def getSchemaRegistryClient: IO[SchemaRegistryClient] = IO.raiseError(new Exception)
          override def getLatestSchemaBySubject(subject: String): IO[Option[Schema]] = IO.pure(None)
          override def getSchemaFor(subject: String, schemaVersion: SchemaVersion): IO[Option[Schema]] = IO.pure(None)
          override def deleteSchemaSubject(subject: String): IO[Unit] = IO.pure(())
        }

      val schemaRegistryState = Map("subject-key" -> 1)
      for {
        ref    <- Ref[IO].of(TestState(schemaRegistryState))
        ts     <- initTestServices(schemaRegistry = getSchemaRegistry(ref).some)
        _      <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, valueSchema), topicDetails, true).attempt
        result <- ref.get
      } yield result.schemas shouldBe schemaRegistryState
    }

    "create the topic in Kafka" in {
      for {
        ts    <- initTestServices()
        _     <- ts.program.createTopic(subject, topicMetadataRequest, topicDetails, true)
        topic <- ts.kafka.describeTopic(subject.value)
      } yield topic.get shouldBe Topic(subject.value, 1)
    }

    s"[pre-cutoff-date] required fields in value schema of a topic can have a default value" in {
      implicit val createdDate: Instant = dateStringToInstant("20230101")

      createTopic(createdAtDefaultValue = Some(123), updatedAtDefaultValue = Some(456)).attempt.map(_ shouldBe Right())
      createTopic(createdAtDefaultValue = Some(123), updatedAtDefaultValue = None).attempt.map(_ shouldBe Right())
      createTopic(createdAtDefaultValue = None, updatedAtDefaultValue = Some(456)).attempt.map(_ shouldBe Right())
    }

    s"[on-cutoff-date] required fields in value schema of a topic can have a default value" in {
      implicit val createdDate: Instant = defaultLoopHoleCutoffDate

      createTopic(createdAtDefaultValue = Some(123), updatedAtDefaultValue = Some(456)).attempt.map(_ shouldBe Right())
      createTopic(createdAtDefaultValue = Some(123), updatedAtDefaultValue = None).attempt.map(_ shouldBe Right())
      createTopic(createdAtDefaultValue = None, updatedAtDefaultValue = Some(456)).attempt.map(_ shouldBe Right())
    }

    s"[post-cutoff-date] required fields in value schema of a topic cannot have a default value - createdAt & updatedAt" in {
      implicit val createdDate: Instant = dateStringToInstant("20230620")
      val createdAt = Some(123L)
      val updatedAt = Some(456L)
      val schema = getSchema("val", createdAt, updatedAt)

      createTopic(createdAtDefaultValue = createdAt, updatedAtDefaultValue = updatedAt).attempt.map(_ shouldBe
        ValidationCombinedErrors(List(
          RequiredSchemaValueFieldWithDefaultValueError("createdAt", schema, "Entity").message,
          RequiredSchemaValueFieldWithDefaultValueError("updatedAt", schema, "Entity").message
        )).asLeft)
    }

    s"[post-cutoff-date] required fields in value schema of a topic cannot have a default value - createdAt" in {
      implicit val createdDate: Instant = dateStringToInstant("20230620")
      val createdAt = Some(123L)
      val schema = getSchema("val", createdAt, None)

      createTopic(createdAtDefaultValue = createdAt, updatedAtDefaultValue = None).attempt.map(_ shouldBe
        RequiredSchemaValueFieldWithDefaultValueError("createdAt", schema, "Entity").asLeft)
    }

    s"[post-cutoff-date] required fields in value schema of a topic cannot have a default value - updateAt" in {
      implicit val createdDate: Instant = dateStringToInstant("20230620")
      val updatedAt = Some(456L)
      val schema = getSchema("val", None, updatedAt)

      createTopic(createdAtDefaultValue = None, updatedAtDefaultValue = updatedAt).attempt.map(_ shouldBe
        RequiredSchemaValueFieldWithDefaultValueError("updatedAt", schema, "Entity").asLeft)
    }

    s"[post-cutoff-date] accept a topic where the required fields do not have a default value" in {
      implicit val createdDate: Instant = dateStringToInstant("20230620")

      createTopic(createdAtDefaultValue = None, updatedAtDefaultValue = None).attempt.map(_ shouldBe Right())
    }

    "ingest metadata into the metadata topic" in {
      for {
        publishTo     <- Ref[IO].of(Map.empty[String, (GenericRecord, Option[GenericRecord], Option[Headers])])
        topicMetadata <- TopicMetadataV2.encode[IO](topicMetadataKey, Some(topicMetadataValue))
        ts            <- initTestServices(new TestKafkaClientAlgebraWithPublishTo(publishTo).some)
        _             <- ts.program.createTopic(subject, topicMetadataRequest, topicDetails, true)
        published     <- publishTo.get
      } yield published shouldBe Map(metadataTopic -> (topicMetadata._1, topicMetadata._2, None))
    }

    "ingest updated metadata into the metadata topic - verify created date did not change" in {
      val updatedRequest = createTopicMetadataRequest(keySchema, valueSchema, "updated@email.com", Instant.ofEpochSecond(0))
      val updatedValue   = updatedRequest.toValue
      for {
        publishTo   <- Ref[IO].of(Map.empty[String, (GenericRecord, Option[GenericRecord], Option[Headers])])
        consumeFrom <- Ref[IO].of(Map.empty[Subject, TopicMetadataContainer])
        metadata    <- IO(new TestMetadataAlgebraWithPublishTo(consumeFrom))
        m           <- TopicMetadataV2.encode[IO](topicMetadataKey, Some(topicMetadataValue))
        updatedM    <- TopicMetadataV2.encode[IO](topicMetadataKey, Some(updatedValue.copy(createdDate = topicMetadataValue.createdDate)))
        ts          <- initTestServices(new TestKafkaClientAlgebraWithPublishTo(publishTo).some, metadata.some)
        _           <- ts.program.createTopic(subject, topicMetadataRequest, TopicDetails(1, 1, 1), true)
        _           <- metadata.addToMetadata(subject, topicMetadataRequest)
        metadataMap <- publishTo.get
        _           <- ts.program.createTopic(subject, updatedRequest, TopicDetails(1, 1, 1), true)
        updatedMap  <- publishTo.get
      } yield {
        metadataMap shouldBe Map(metadataTopic -> (m._1, m._2, None))
        updatedMap shouldBe Map(metadataTopic -> (updatedM._1, updatedM._2, None))
      }
    }

    "rollback kafka topic creation when error encountered in publishing metadata" in {
      for {
        publishTo   <- Ref[IO].of(Map.empty[String, (GenericRecord, Option[GenericRecord], Option[Headers])])
        kafkaClient = new TestKafkaClientAlgebraWithPublishTo(publishTo, failOnPublish = true)
        ts          <- initTestServices(kafkaClient.some)
        _           <- ts.program.createTopic(subject, topicMetadataRequest, topicDetails, true).attempt
        topic       <- ts.kafka.describeTopic(subject.value)
      } yield topic.isDefined shouldBe false
    }

    "not delete an existing topic when rolling back" in {
      for {
        publishTo   <- Ref[IO].of(Map.empty[String, (GenericRecord, Option[GenericRecord], Option[Headers])])
        kafkaClient = new TestKafkaClientAlgebraWithPublishTo(publishTo, failOnPublish = true)
        ts          <- initTestServices(kafkaClient.some)
        _           <- ts.kafka.createTopic(subject.value, topicDetails)
        _           <- ts.program.createTopic(subject, topicMetadataRequest, topicDetails, true).attempt
        topic       <- ts.kafka.describeTopic(subject.value)
      } yield topic.isDefined shouldBe true
    }

    "ingest updated metadata into the metadata topic - verify deprecated date if supplied is not overwritten" in {
      val request        = createTopicMetadataRequest(keySchema, valueSchema, deprecated = true, deprecatedDate = Some(Instant.now))
      val updatedRequest = createTopicMetadataRequest(keySchema, valueSchema, "updated@email.com", deprecated = true)
      for {
        publishTo   <- Ref[IO].of(Map.empty[String, (GenericRecord, Option[GenericRecord], Option[Headers])])
        consumeFrom <- Ref[IO].of(Map.empty[Subject, TopicMetadataContainer])
        metadata    <- IO(new TestMetadataAlgebraWithPublishTo(consumeFrom))
        ts          <- initTestServices(new TestKafkaClientAlgebraWithPublishTo(publishTo).some, metadata.some)
        _           <- ts.program.createTopic(subject, request, topicDetails, true)
        _           <- metadata.addToMetadata(subject, request)
        metadataMap <- publishTo.get
        _           <- ts.program.createTopic(subject, updatedRequest, topicDetails, true)
        updatedMap  <- publishTo.get
      } yield {
        updatedMap(metadataTopic)._2.get.get("deprecatedDate") shouldBe metadataMap(metadataTopic)._2.get.get("deprecatedDate")
      }
    }

    "ingest updated metadata into the metadata topic - verify deprecated date is updated when starting with None" in {
      val request = createTopicMetadataRequest(keySchema, valueSchema)
      val updatedRequest = createTopicMetadataRequest(keySchema, valueSchema, "updated@email.com", deprecated = true)
      for {
        publishTo   <- Ref[IO].of(Map.empty[String, (GenericRecord, Option[GenericRecord], Option[Headers])])
        consumeFrom <- Ref[IO].of(Map.empty[Subject, TopicMetadataContainer])
        metadata    <- IO(new TestMetadataAlgebraWithPublishTo(consumeFrom))
        ts          <- initTestServices(new TestKafkaClientAlgebraWithPublishTo(publishTo).some, metadata.some)
        _           <- ts.program.createTopic(subject, request, topicDetails, true)
        _           <- metadata.addToMetadata(subject, request)
        metadataMap <- publishTo.get
        _           <- ts.program.createTopic(subject, updatedRequest, topicDetails, true)
        updatedMap  <- publishTo.get
      } yield {
        val ud = metadataMap(metadataTopic)._2.get.get("deprecatedDate")
        val dd = updatedMap(metadataTopic)._2.get.get("deprecatedDate")
        ud shouldBe null
        Instant.parse(dd.toString) shouldBe a[Instant]
      }
    }

    "create topic with custom number of partitions" in {
      for {
        ts      <- initTestServices()
        request = createTopicMetadataRequest(keySchema, valueSchema, numPartitions = refineMV[TopicMetadataV2Request.NumPartitionsPredicate](22).some)
        _       <- ts.program.createTopic(subject, request, topicDetails)
        topic   <- ts.kafka.describeTopic(subject.value)
      } yield topic.get shouldBe Topic(subject.value, 22)
    }

    "throw error on topic with key and value field named same but with different type" in {
      val mismatchedValueSchema =
        SchemaBuilder
          .record("name")
          .fields()
          .name("isTrue")
          .doc("text")
          .`type`()
          .booleanType()
          .noDefault()
          .name(RequiredField.CREATED_AT)
          .doc("text")
          .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .name(RequiredField.UPDATED_AT)
          .doc("text")
          .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .endRecord()

      val result = for {
        ts <- initTestServices()
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, mismatchedValueSchema), topicDetails, true)
      } yield ()

      val keyFieldSchema   = keySchema.getField("isTrue").schema()
      val valueFieldSchema = mismatchedValueSchema.getField("isTrue").schema()
      result.attempt.map(_ shouldBe IncompatibleKeyAndValueFieldNamesError("isTrue", keyFieldSchema, valueFieldSchema).asLeft)
    }

    "throw error on topic with key that has field of type union [null, ...]" in {
      val union = SchemaBuilder.unionOf().nullType().and().stringType().endUnion()
      val recordWithNullDefaultKey =
        SchemaBuilder
          .record("name")
          .fields()
          .name("nullableUnion")
          .doc("text")
          .`type`(union)
          .withDefault(null)
          .endRecord()

      val recordWithNullDefaultVal =
        SchemaBuilder
          .record("name")
          .fields()
          .name("nullableUnion")
          .doc("text")
          .`type`(union)
          .withDefault(null)
          .name(RequiredField.CREATED_AT)
          .doc("text")
          .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .name(RequiredField.UPDATED_AT)
          .doc("text")
          .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .endRecord()

      val result = for {
        ts <- initTestServices()
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(recordWithNullDefaultKey, recordWithNullDefaultVal), topicDetails, true)
      } yield ()

      val fieldSchema = recordWithNullDefaultVal.getField("nullableUnion").schema()
      result.attempt.map(_ shouldBe KeyHasNullableFieldError("nullableUnion", fieldSchema).asLeft)
    }

    "do not throw error on topic with key that has field of type union [null, ...] if streamType is 'Event'" in {
      val union = SchemaBuilder.unionOf().nullType().and().stringType().endUnion()
      val recordWithNullDefaultKey =
        SchemaBuilder
          .record("name")
          .fields()
          .name("nullableUnion")
          .doc("text")
          .`type`(union)
          .withDefault(null)
          .endRecord()

      val recordWithNullDefaultVal =
        SchemaBuilder
          .record("name")
          .fields()
          .name("nullableUnion")
          .doc("text")
          .`type`(union)
          .withDefault(null)
          .name(RequiredField.CREATED_AT)
          .doc("text")
          .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .name(RequiredField.UPDATED_AT)
          .doc("text")
          .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .endRecord()

      for {
        ts <- initTestServices()
        _  <- ts.program.createTopic(subject, createEventStreamTypeTopicMetadataRequest(recordWithNullDefaultKey, recordWithNullDefaultVal), topicDetails, true)
      } yield succeed
    }

    "throw error on topic with key that has field of type null" in {
      val recordWithNullTypeKey =
        SchemaBuilder
          .record("name")
          .fields()
          .name("nullableField")
          .doc("text")
          .`type`("null")
          .noDefault()
          .endRecord()

      val recordWithNullTypeVal =
        SchemaBuilder
          .record("name")
          .fields()
          .name("nullableField")
          .doc("text")
          .`type`("null")
          .noDefault()
          .name(RequiredField.CREATED_AT)
          .doc("text")
          .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .name(RequiredField.UPDATED_AT)
          .doc("text")
          .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .endRecord()

      val result = for {
        ts <- initTestServices()
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(recordWithNullTypeKey, recordWithNullTypeVal), topicDetails, true)
      } yield ()

      val fieldSchema = recordWithNullTypeKey.getField("nullableField").schema()
      result.attempt.map(_ shouldBe KeyHasNullableFieldError("nullableField", fieldSchema).asLeft)
    }

    "do not throw error on topic with key that has field of type null if streamType is 'Event'" in {
      val recordWithNullTypeKey =
        SchemaBuilder
          .record("name")
          .fields()
          .name("nullableField")
          .doc("text")
          .`type`("null")
          .noDefault()
          .endRecord()

      val recordWithNullTypeVal =
        SchemaBuilder
          .record("name")
          .fields()
          .name("nullableField")
          .doc("text")
          .`type`("null")
          .noDefault()
          .name(RequiredField.CREATED_AT)
          .doc("text")
          .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .name(RequiredField.UPDATED_AT)
          .doc("text")
          .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .endRecord()

      for {
        ts <- initTestServices()
        _  <- ts.program.createTopic(subject, createEventStreamTypeTopicMetadataRequest(recordWithNullTypeKey, recordWithNullTypeVal), topicDetails, true)
      } yield succeed
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
          |        "default": "abc",
          |        "doc": "text",
          |        "type":[
          |                 {
          |                   "type": "string",
          |                   "logicalType": "uuid"
          |                 },
          |                 "null"
          |               ]
          |     },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
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
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "context",
          |       "doc": "text",
          |       "default": "abc",
          |       "type": ["string", "null" ]
          |     },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin
      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      val result = for {
        ts <- initTestServices()
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, firstValueSchema), topicDetails, true)
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, valueSchemaEvolution), topicDetails, true)
      } yield ()

      result.attempt.map(_ shouldBe IllegalLogicalTypeChangeError("uuid", "null", "context").asLeft)
    }

    "throw error on schema evolution with illegal union logical type addition" in {
      val firstValue =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "context",
          |       "default": "abc",
          |       "type": ["string", "null" ],
          |       "doc": "text"
          |     },
          |    {
          |      "name": "createdAt",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      },
          |      "doc": "text"
          |    },
          |    {
          |      "name": "updatedAt",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      },
          |      "doc": "text"
          |    }
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
          |        "default": "abc",
          |        "doc": "text",
          |        "type":[
          |                 {
          |                   "type": "string",
          |                   "logicalType": "uuid"
          |                 },
          |                 "null"
          |               ]
          |     },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    }
          |  ]
          |}
        """.stripMargin

      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      val result = for {
        ts <- initTestServices()
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, firstValueSchema), topicDetails, true)
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, valueSchemaEvolution), topicDetails, true)
      } yield ()

      result.attempt.map(_ shouldBe IllegalLogicalTypeChangeError("null", "uuid", "context").asLeft)
    }

    "throw error on schema evolution with illegal union logical type change" in {
      val firstValue =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |        "name":"context",
          |        "default": "abc",
          |        "doc": "text",
          |        "type":[
          |                 {
          |                   "type": "string",
          |                   "logicalType": "uuid"
          |                 },
          |                 "null"
          |               ]
          |     },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
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
          |  "name": "test",
          |  "fields": [
          |     {
          |        "name":"context",
          |        "default": "abc",
          |        "doc": "text",
          |        "type":[
          |                 {
          |                   "type": "string",
          |                   "logicalType": "date"
          |                 },
          |                 "null"
          |               ]
          |     },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    }
          |  ]
          |}
        """.stripMargin

      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      val result = for {
        ts <- initTestServices()
        _ <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, firstValueSchema), topicDetails, true)
        _ <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, valueSchemaEvolution), topicDetails, true)
      } yield ()

      result.attempt.map(_ shouldBe IllegalLogicalTypeChangeError("uuid", "date", "context").asLeft)
    }

    "throw error on schema evolution with illegal key field logical type change string" in {
      val firstKey =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "fields": [
          |    {
          |      "name": "keyThing",
          |      "doc": "text",
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
          |      "doc": "text",
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

      val result = for {
        ts <- initTestServices()
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(firstKeySchema, valueSchema), topicDetails, true)
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchemaEvolution, valueSchema), topicDetails, true)
      } yield ()

      result.attempt.map(_ shouldBe IllegalLogicalTypeChangeError("uuid", "date", "keyThing").asLeft)
    }

    "throw error on schema evolution with illegal value field logical type removal" in {
      val firstValue =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "fields": [
          |    {
          |      "name": "valueThing",
          |      "doc": "text",
          |      "type":{
          |        "type": "string",
          |        "logicalType":"uuid"
          |      }
          |    },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
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
          |      "doc": "text",
          |      "type": "string"
          |    },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin

      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      val result = for {
        ts <- initTestServices()
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, firstValueSchema), topicDetails, true)
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, valueSchemaEvolution), topicDetails, true)
      } yield ()

      result.attempt.map(_ shouldBe IllegalLogicalTypeChangeError("uuid", "null", "valueThing").asLeft)
    }

    "throw error on schema evolution with illegal value array with field logical type removal" in {
      val firstValue =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "ArrayOfThings",
          |       "doc": "text",
          |       "type": {
          |         "type": "array",
          |         "items":
          |           {
          |             "logicalType": "date",
          |             "type": "int"
          |           }
          |       }
          |     },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
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
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "ArrayOfThings",
          |       "doc": "text",
          |       "type": {
          |         "type": "array",
          |         "items":
          |           {
          |             "type": "int"
          |           }
          |       }
          |     },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin

      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      val result = for {
        ts <- initTestServices()
        _ <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, firstValueSchema), topicDetails, true)
        _ <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, valueSchemaEvolution), topicDetails, true)
      } yield ()

      result.attempt.map(_ shouldBe IllegalLogicalTypeChangeError("date", "null", "ArrayOfThings").asLeft)
    }

    "throw error on schema evolution with illegal value map with field logical type removal" in {
      val firstValue =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "MapOfThings",
          |       "doc": "text",
          |       "type": {
          |         "type": "map",
          |         "values":
          |           {
          |             "logicalType": "date",
          |             "type": "int"
          |           }
          |       }
          |     },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
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
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "MapOfThings",
          |       "doc": "text",
          |       "type": {
          |         "type": "map",
          |         "values":
          |           {
          |             "type": "int"
          |           }
          |       }
          |     },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin

      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      val result = for {
        ts <- initTestServices()
        _ <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, firstValueSchema), topicDetails, true)
        _ <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, valueSchemaEvolution), topicDetails, true)
      } yield ()

      result.attempt.map(_ shouldBe IllegalLogicalTypeChangeError("date", "null", "MapOfThings").asLeft)
    }

    "do not throw logical type validation error on schema evolution with no change, array" in {
      val firstValue =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "ArrayOfThings",
          |       "doc": "text",
          |       "type": {
          |         "type": "array",
          |         "items":
          |           {
          |             "logicalType": "date",
          |             "type": "int"
          |           }
          |       }
          |     },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
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
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "ArrayOfThings",
          |       "doc": "text",
          |       "type": {
          |         "type": "array",
          |         "items":
          |           {
          |             "logicalType": "date",
          |             "type": "int"
          |           }
          |       }
          |     },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin

      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      for {
        ts <- initTestServices()
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, firstValueSchema), topicDetails, true)
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, valueSchemaEvolution), topicDetails, true)
      } yield succeed
    }

    "do not throw logical type validation error on schema evolution with no change, map" in {
      val firstValue =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "ArrayOfThings",
          |       "doc": "text",
          |       "type": {
          |         "type": "map",
          |         "values":
          |           {
          |             "logicalType": "date",
          |             "type": "int"
          |           }
          |       }
          |     },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
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
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "ArrayOfThings",
          |       "doc": "text",
          |       "type": {
          |         "type": "map",
          |         "values":
          |           {
          |             "logicalType": "date",
          |             "type": "int"
          |           }
          |       }
          |     },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin

      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      for {
        ts <- initTestServices()
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, firstValueSchema), topicDetails, true)
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, valueSchemaEvolution), topicDetails, true)
      } yield succeed
    }

    "do not throw logical type validation error on schema evolution with no change, nested record" in {
      val firstValue =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "RecordOfThings",
          |       "doc": "text",
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
          |     },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
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
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "RecordOfThings",
          |       "doc": "text",
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
          |     },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin

      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      for {
        ts <- initTestServices()
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, firstValueSchema), topicDetails, true)
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, valueSchemaEvolution), topicDetails, true)
      } yield succeed
    }

    "throw error on schema evolution with illegal key field logical type change within a nested record" in {
      val firstKey =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "RecordOfThings",
          |       "doc": "text",
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
          |       "doc": "text",
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

      val result = for {
        ts <- initTestServices()
        _ <- ts.program.createTopic(subject, createTopicMetadataRequest(firstKeySchema, valueSchema), topicDetails, true)
        _ <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchemaEvolution, valueSchema), topicDetails, true)
      } yield ()

      result.attempt.map(_ shouldBe IllegalLogicalTypeChangeError("uuid", "null", "address").asLeft)
    }

    "throw error on schema evolution with illegal key field logical type change" in {
      val firstKey =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "fields": [
          |    {
          |      "name": "keyThing",
          |      "doc": "text",
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
          |      "doc": "text",
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

      val result = for {
        ts <- initTestServices()
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(firstKeySchema, valueSchema), topicDetails, true)
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchemaEvolution, valueSchema), topicDetails, true)
      } yield ()

      result.attempt.map(_ shouldBe IllegalLogicalTypeChangeError("timestamp-millis", "timestamp-micros", "keyThing").asLeft)
    }

    "throw error on schema evolution with illegal value field logical type change" in {
      val firstValue =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "fields": [
          |    {
          |      "name": "valueThing",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
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
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-micros"
          |      }
          |    },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin

      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      val result = for {
        ts <- initTestServices()
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, firstValueSchema), topicDetails, true)
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, valueSchemaEvolution), topicDetails, true)
      } yield ()

      result.attempt.map(_ shouldBe IllegalLogicalTypeChangeError("timestamp-millis", "timestamp-micros", "valueThing").asLeft)
    }

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
          |      "doc": "text",
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
          |      "doc": "text",
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

      val result = for {
        ts <- initTestServices()
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(firstKeySchema, valueSchema), topicDetails, true)
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchemaEvolution, valueSchema), topicDetails, true)
      } yield ()

      result.attempt.map(_ shouldBe IllegalLogicalTypeChangeError("null", "uuid", "valueThing").asLeft)
    }

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
          |      "doc": "text",
          |      "type": "string"
          |    },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
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
          |  "namespace": "dvs.data_platform.dvs_sandbox",
          |  "fields": [
          |    {
          |      "name": "valueThing",
          |      "doc": "text",
          |      "type":{
          |        "type": "string",
          |        "logicalType":"uuid"
          |      }
          |    },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin

      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      val result = for {
        ts <- initTestServices()
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, firstValueSchema), topicDetails, true)
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, valueSchemaEvolution), topicDetails, true)
      } yield ()

      result.attempt.map(_ shouldBe IllegalLogicalTypeChangeError("null", "uuid", "valueThing").asLeft)
    }

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
          |     "doc": "text",
          |			"type": {
          |            "type": "enum",
          |            "name": "test_type",
          |            "symbols": ["test1", "test2"]
          |        }
          |		},
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
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
          |  "namespace": "dvs.data_platform.dvs_sandbox",
          |  "fields": [
          |    {
          |			"name": "testEnum",
          |     "doc": "text",
          |			"type": {
          |            "type": "enum",
          |            "name": "test_type",
          |            "symbols": ["test1", "test2"]
          |        }
          |		},
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin

      val firstValueSchema = new Schema.Parser().parse(firstValue)
      val valueSchemaEvolution = new Schema.Parser().parse(valueEvolution)

      for {
        ts <- initTestServices()
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, firstValueSchema), topicDetails, true)
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, valueSchemaEvolution), topicDetails, true)
      } yield succeed
    }

    "throw error on value schema evolution with missing required field doc" in {
      val value =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "fields": [
          |      {
          |      "name": "updatedAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "createdAt",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin

      val valueSchema = new Schema.Parser().parse(value)

      val result = for {
        ts <- initTestServices()
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(getSchema("key"), valueSchema), topicDetails, true)
      } yield ()

      result.attempt.map(_ shouldBe RequiredSchemaValueFieldMissingError(RequiredField.DOC, valueSchema, "Entity").asLeft)
    }

    "throw error on value schema evolution with missing required fields updatedAt" in {
      val value =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "fields": [
          |      {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin

      val valueSchema = new Schema.Parser().parse(value)

      val result = for {
        ts <- initTestServices()
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(getSchema("key"), valueSchema), topicDetails, true)
      } yield ()

      result.attempt.map(_ shouldBe RequiredSchemaValueFieldMissingError(RequiredField.UPDATED_AT, valueSchema, "Entity").asLeft)
    }

    "successfully validate topic schema with value that has field of type union [null, ...]" in {
      val union = SchemaBuilder.unionOf().nullType().and().stringType().endUnion()
      val recordWithNullDefault =
        SchemaBuilder
          .record("name")
          .fields()
          .name("nullableUnion")
          .doc("text")
          .`type`(union)
          .withDefault(null)
          .name(RequiredField.CREATED_AT)
          .doc("text")
          .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .name(RequiredField.UPDATED_AT)
          .doc("text")
          .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .endRecord()

      for {
        ts <- Resource.eval(initTestServices())
        _  <- ts.program.registerSchemas(subject ,keySchema, recordWithNullDefault)
        _  <- ts.program.createTopicResource(subject, topicDetails)
        _  <- Resource.eval(ts.program.createTopicFromMetadataOnly(subject, createTopicMetadataRequest(keySchema, recordWithNullDefault), true))
      } yield succeed
    }

    "successfully validate topic schema with value that has field of type null" in {
      val recordWithNullType =
        SchemaBuilder
          .record("name")
          .fields()
          .name("nullableField")
          .doc("text")
          .`type`("null")
          .noDefault()
          .name(RequiredField.CREATED_AT)
          .doc("text")
          .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .name(RequiredField.UPDATED_AT)
          .doc("text")
          .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .endRecord()

      for {
        ts <- Resource.eval(initTestServices())
        _  <- ts.program.registerSchemas(subject, keySchema, recordWithNullType)
        _  <- ts.program.createTopicResource(subject, topicDetails)
        _  <- Resource.eval(ts.program.createTopicFromMetadataOnly(subject, createTopicMetadataRequest(keySchema, recordWithNullType), true))
      } yield succeed
    }

    "successfully validate topic schema with key that has field of type union [not null, not null]" in {
      val union = SchemaBuilder.unionOf().intType().and().stringType().endUnion()
      val recordWithNullDefault =
        SchemaBuilder
          .record("name")
          .fields()
          .name("nullableUnion")
          .doc("text")
          .`type`(union)
          .withDefault(5)
          .endRecord()

      for {
        ts <- Resource.eval(initTestServices())
        _  <- ts.program.registerSchemas(subject, recordWithNullDefault, valueSchema)
        _  <- ts.program.createTopicResource(subject, topicDetails)
        _  <- Resource.eval(ts.program.createTopicFromMetadataOnly(subject, createTopicMetadataRequest(recordWithNullDefault, valueSchema), true))
      } yield succeed
    }

    "successfully evolve schema which had mismatched types in past version" ignore {
      val mismatchedValueSchema =
        SchemaBuilder
          .record("name")
          .fields()
          .name("isTrue")
          .doc("text")
          .`type`().booleanType().noDefault()
          .name(RequiredField.CREATED_AT)
          .doc("text")
          .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .name(RequiredField.UPDATED_AT)
          .doc("text")
          .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .endRecord()
      val mismatchedValueSchemaEvolution =
        SchemaBuilder
          .record("name")
          .fields()
          .name("isTrue")
          .doc("text")
          .`type`().booleanType().noDefault().nullableInt("nullInt", 12)
          .name(RequiredField.CREATED_AT)
          .doc("text")
          .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .name(RequiredField.UPDATED_AT)
          .doc("text")
          .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .endRecord()

      for {
        ts <- Resource.eval(initTestServices())
        _ <- ts.program.registerSchemas(subject ,keySchema, mismatchedValueSchema)
        _ <- ts.program.createTopicResource(subject, topicDetails)
        _ <- Resource.eval(ts.program.createTopicFromMetadataOnly(subject, createTopicMetadataRequest(keySchema, mismatchedValueSchema), true))
        _ <- Resource.eval(ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, mismatchedValueSchemaEvolution), topicDetails, true))
      } yield succeed
    }

    "throw error if key schema is not registered as record type before creating topic from metadata only" in {
      val incorrectKeySchema = new Schema.Parser().parse("""
                                                           |{
                                                           |	"type": "string"
                                                           |}""".stripMargin)
      val result = for {
        ts <- Resource.eval(initTestServices())
        _  <- ts.program.registerSchemas(subject, incorrectKeySchema, valueSchema)
        _  <- ts.program.createTopicResource(subject, topicDetails)
        _  <- Resource.eval(ts.program.createTopicFromMetadataOnly(subject, createTopicMetadataRequest(incorrectKeySchema, valueSchema)))
      } yield ()

      result.attempt.map(_ shouldBe TopicSchemaError.InvalidSchemaTypeError.asLeft)
    }

    "throw error if value schema is not registered as record type before creating topic from metadata only" in {
      val incorrectValueSchema = new Schema.Parser().parse("""
                                                             |{
                                                             |	"type": "string"
                                                             |}""".stripMargin)
      val result = for {
        ts <- Resource.eval(initTestServices())
        _  <- ts.program.registerSchemas(subject, keySchema, incorrectValueSchema)
        _  <- ts.program.createTopicResource(subject, topicDetails)
        _  <- Resource.eval(ts.program.createTopicFromMetadataOnly(subject, createTopicMetadataRequest(keySchema, incorrectValueSchema)))
      } yield ()

      result.attempt.map(_ shouldBe TopicSchemaError.InvalidSchemaTypeError.asLeft)
    }

    "successfully creating topic from metadata only where key and value schemas are records" in {
      for {
        ts <- Resource.eval(initTestServices())
        _  <- ts.program.registerSchemas(subject ,keySchema, valueSchema)
        _  <- ts.program.createTopicResource(subject, topicDetails)
        _  <- Resource.eval(ts.program.createTopicFromMetadataOnly(subject, topicMetadataRequest))
      } yield succeed
    }

    "throw error creating topic from metadata only where topic doesn't exist" in {
      val result = for {
        ts <- initTestServices()
        _  <- ts.program.createTopicFromMetadataOnly(subject, createTopicMetadataRequest(keySchema, valueSchema))
      } yield ()

      result.attempt.map(_ shouldBe MetadataOnlyTopicDoesNotExist(subject.value).asLeft)
    }


    "throw error of schema nullable values don't have default value" in {
      val union = SchemaBuilder.unionOf().nullType().and().stringType().endUnion()
      val nullableValue = SchemaBuilder
                          .record("val")
                          .fields()
                          .name("itsnullable")
                          .doc("text")
                          .`type`(union)
                          .noDefault()
                          .name(RequiredField.CREATED_AT)
                          .doc("text")
                          .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
                          .noDefault()
                          .name(RequiredField.UPDATED_AT)
                          .doc("text")
                          .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
                          .noDefault()
                          .endRecord()

      val result = for {
        ts <- initTestServices()
        _  <- ts.program.createTopic(subject, createTopicMetadataRequest(keySchema, nullableValue), topicDetails, true)
      } yield ()

      result.attempt.map(_ shouldBe NullableFieldWithoutDefaultValueError("itsnullable", nullableValue.getFields.asScala.head.schema()).asLeft)
    }

    "do not throw error on topic with key that has field with non-record type if topic contains KSQL tag" in {
      val stringType =
        """
          |{
          |  "type": "string",
          |  "doc": "text",
          |  "name": "test"
          |}
        """.stripMargin
      val stringTypeKeySchema = new Schema.Parser().parse(stringType)
      for {
        ts <- initTestServices()
        _  <- ts.program.createTopic(subject,
          createEventStreamTypeTopicMetadataRequest(stringTypeKeySchema, valueSchema, tags = List("KSQL")),
          topicDetails)
      } yield succeed
    }

    "throw error on topic with key that has field with non-record type if topic doesn't contain KSQL tag" in {
      val stringType =
        """
          |{
          |  "type": "string",
          |  "doc": "text",
          |  "name": "test"
          |}
        """.stripMargin
      val stringTypeKeySchema = new Schema.Parser().parse(stringType)
      val result = for {
        ts <- initTestServices()
        _  <- ts.program.createTopic(subject,
          createEventStreamTypeTopicMetadataRequest(stringTypeKeySchema, valueSchema),
          topicDetails)
      } yield ()

      result.attempt.map(_ shouldBe InvalidSchemaTypeError.asLeft)
    }

    "throw error if schema with key that has field of logical type iso-datetime" in {
      val key =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "fields": [
          |    {
          |      "name": "timestamp",
          |      "doc": "text",
          |      "type":{
          |        "type": "string",
          |        "logicalType": "iso-datetime"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin
      val keySchema = new Schema.Parser().parse(key)

      val result = for {
        ts <- initTestServices()
        _ <- ts.program.createTopic(
          subject,
          createTopicMetadataRequest(keySchema, valueSchema),
          topicDetails,
          true
        )
      } yield ()

      result.attempt.map(_ shouldBe UnsupportedLogicalType(keySchema.getField("timestamp"), "iso-datetime").asLeft)
    }

    "throw error if schema with value that has field of logical type iso-datetime" in {
      val value =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "fields": [
          |    {
          |      "name": "timestamp",
          |      "type":{
          |        "type": "string",
          |        "logicalType": "iso-datetime"
          |      },
          |      "doc": "text"
          |    },
          |    {
          |      "name": "createdAt",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      },
          |      "doc": "text"
          |    },
          |    {
          |      "name": "updatedAt",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      },
          |      "doc": "text"
          |    }
          |  ]
          |}
      """.stripMargin
      val valueSchema = new Schema.Parser().parse(value)

      val result = for {
        ts <- initTestServices()
        _ <- ts.program.createTopic(
          subject,
          createTopicMetadataRequest(keySchema, valueSchema),
          topicDetails,
          true
        )
      } yield ()

      result.attempt.map(_ shouldBe UnsupportedLogicalType(valueSchema.getField("timestamp"), "iso-datetime").asLeft)
    }

    def createTopic(createdAtDefaultValue: Option[Long], updatedAtDefaultValue: Option[Long])(implicit createdDate: Instant) =
      for {
        m <- TestMetadataAlgebra()
        _ <- TopicUtils.updateTopicMetadata(List(subject.value), m, createdDate)
        ts <- initTestServices(metadataAlgebraOpt = Some(m))
        _ <- ts.program.createTopic(subject, createTopicMetadataRequest(createdAtDefaultValue, updatedAtDefaultValue), topicDetails, true)
      } yield ()
  }

  type Record = (GenericRecord, Option[GenericRecord], Option[Headers])

  private final class TestKafkaClientAlgebraWithPublishTo(publishTo: Ref[IO, Map[TopicName, Record]], failOnPublish: Boolean = false)
    extends KafkaClientAlgebra[IO] {

    override def consumeSafelyStringKeyMessagesWithOffsetInfo(topicName: TopicName, consumerGroup: ConsumerGroup, commitOffsets: Boolean): fs2.Stream[IO, Either[Throwable, ((Option[String], Option[GenericRecord], Option[Headers]), (Partition, Offset))]] = fs2.Stream.empty

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

    override def consumeSafelyMessages(topicName: TopicName, consumerGroup: ConsumerGroup, commitOffsets: Boolean): fs2.Stream[IO, Either[Throwable, (GenericRecord, Option[GenericRecord], Option[Headers])]] = fs2.Stream.empty

    override def consumeSafelyWithOffsetInfo(topicName: TopicName, consumerGroup: ConsumerGroup, commitOffsets: Boolean): fs2.Stream[IO, Either[Throwable, ((GenericRecord, Option[GenericRecord], Option[Headers]), (Partition, Offset))]] = fs2.Stream.empty

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
}

object CreateTopicProgramSpec extends NotificationsTestSuite {
  val keySchema     = getSchema("key")
  val valueSchema   = getSchema("val")
  val metadataTopic = "dvs.test-metadata-topic"

  val subject              = Subject.createValidated("dvs.subject").get
  val topicMetadataRequest = createTopicMetadataRequest(keySchema, valueSchema)
  val topicDetails         = TopicDetails(1, 1, 1)
  val topicMetadataKey     = TopicMetadataV2Key(subject)
  val topicMetadataValue   = topicMetadataRequest.toValue

  val defaultLoopHoleCutoffDate: Instant = dateStringToInstant("20230619")

  implicit val contextShift: ContextShift[IO]   = IO.contextShift(ExecutionContext.global)
  implicit val concurrentEffect: Concurrent[IO] = IO.ioConcurrentEffect
  implicit val timer: Timer[IO]                 = IO.timer(ExecutionContext.global)

  implicit private def unsafeLogger[F[_]: Sync]: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  implicit private def addOptionalDefaultValue[R](gd: GenericDefault[R]): CustomGenericDefault[R] = new CustomGenericDefault[R](gd)

  case class TestServices(program: CreateTopicProgram[IO], schemaRegistry: SchemaRegistry[IO], kafka: KafkaAdminAlgebra[IO])

  def initTestServices(kafkaClientOpt: Option[KafkaClientAlgebra[IO]] = None,
                       metadataAlgebraOpt: Option[MetadataAlgebra[IO]] = None,
                       schemaRegistry: Option[SchemaRegistry[IO]] = None,
                       retryPolicy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp): IO[TestServices] = {
    for {
      defaultSchemaRegistry <- SchemaRegistry.test[IO]
      kafka                 <- KafkaAdminAlgebra.test[IO]()
      defaultKafkaClient    <- KafkaClientAlgebra.test[IO]
      kafkaClient           = kafkaClientOpt.getOrElse(defaultKafkaClient)
      defaultMetadata       <- metadataAlgebraF(metadataTopic, defaultSchemaRegistry, kafkaClient)
    } yield {
      val createTopicProgram =
        CreateTopicProgram.make[IO](
          schemaRegistry.getOrElse(defaultSchemaRegistry),
          kafka,
          kafkaClient,
          retryPolicy,
          Subject.createValidated(metadataTopic).get,
          metadataAlgebraOpt.getOrElse(defaultMetadata),
          defaultLoopHoleCutoffDate
        )

      TestServices(createTopicProgram, defaultSchemaRegistry, kafka)
    }
  }

  def metadataAlgebraF(metadataTopic: String,
                       schemaRegistry: SchemaRegistry[IO],
                       kafkaClient: KafkaClientAlgebra[IO]): IO[MetadataAlgebra[IO]] = {
    implicit val notificationSenderMock: InternalNotificationSender[IO] = getInternalNotificationSenderMock[IO]
    MetadataAlgebra.make(Subject.createValidated(metadataTopic).get, "consumerGroup", kafkaClient, schemaRegistry, consumeMetadataEnabled = true, Once
    )
  }


  def createTopicMetadataRequest(
                                  keySchema: Schema,
                                  valueSchema: Schema,
                                  email: String = "test@test.com",
                                  createdDate: Instant = Instant.now(),
                                  deprecated: Boolean = false,
                                  deprecatedDate: Option[Instant] = None,
                                  numPartitions: Option[NumPartitions] = None
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
      List.empty,
      Some("notification.url")
    )

  def createEventStreamTypeTopicMetadataRequest(
                                                 keySchema: Schema,
                                                 valueSchema: Schema,
                                                 email: String = "test@test.com",
                                                 createdDate: Instant = Instant.now(),
                                                 deprecated: Boolean = false,
                                                 deprecatedDate: Option[Instant] = None,
                                                 numPartitions: Option[NumPartitions] = None,
                                                 tags: List[String] = List.empty
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
      tags,
      Some("notification.url")
    )

  def getSchema(name: String,
                createdAtDefaultValue: Option[Long] = None,
                updatedAtDefaultValue: Option[Long] = None): Schema = {
    val tempSchema = SchemaBuilder
      .record(name)
      .fields()
      .name("isTrue")
      .doc("text")
      .`type`()
      .stringType()
      .noDefault()

    if (name == "key") {
      tempSchema.endRecord()
    } else {
      tempSchema
        .name(RequiredField.CREATED_AT)
        .doc("text")
        .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .default(createdAtDefaultValue)
        .name(RequiredField.UPDATED_AT)
        .doc("text")
        .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .default(updatedAtDefaultValue)
        .endRecord()
    }
  }

  def createTopicMetadataRequest(createdAtDefaultValue: Option[Long], updatedAtDefaultValue: Option[Long]): TopicMetadataV2Request =
    createTopicMetadataRequest(keySchema, getSchema("val", createdAtDefaultValue, updatedAtDefaultValue))
}

class CustomGenericDefault[R](gd: GenericDefault[R]) {
  def default(defaultValue: Option[Long]): FieldAssembler[R] = defaultValue match {
    case Some(dv) => gd.withDefault(dv)
    case None => gd.noDefault()
  }
}