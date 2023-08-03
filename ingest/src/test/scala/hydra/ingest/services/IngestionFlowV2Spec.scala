package hydra.ingest.services

import cats.effect.{Concurrent, ContextShift, IO}
import cats.syntax.all._
import fs2.kafka.{Header, Headers}
import hydra.avro.registry.SchemaRegistry
import hydra.avro.util.SchemaWrapper
import hydra.core.transport.ValidationStrategy
import hydra.ingest.services.IngestionFlowV2.{AvroConversionAugmentedException, KeyAndValueMismatch, KeyAndValueMismatchedValuesException, V2IngestRequest}
import hydra.ingest.utils.TopicUtils
import hydra.kafka.algebras.{KafkaClientAlgebra, TestMetadataAlgebra}
import hydra.kafka.model.TopicMetadataV2Request.Subject
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scalacache.Cache
import scalacache.guava.GuavaCache

import java.time.Instant
import scala.concurrent.ExecutionContext

final class IngestionFlowV2Spec extends AnyFlatSpec with Matchers {

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val concurrentEffect: Concurrent[IO] = IO.ioConcurrentEffect
  private implicit val mode: scalacache.Mode[IO] = scalacache.CatsEffect.modes.async
  implicit val logger =  Slf4jLogger.getLogger[IO]
  implicit val timer = IO.timer(ExecutionContext.global)

  private val testSubject: Subject = Subject.createValidated("dvs.test.v0.Testing").get
  private val testSubjectV1: Subject = Subject.createValidated("dvs.test.v1.Testing").get
  private val timestampValidationCutoffDate: Instant = Instant.parse("2023-07-11T00:00:00Z")
  private val preTimestampValidationCutoffDate: Instant = Instant.parse("2023-05-30T00:00:00Z")
  private val postTimestampValidationCutoffDate: Instant = Instant.parse("2023-07-30T00:00:00Z")

  private val testKeyPayload: String =
    """{"id": "testing"}"""

  private val testValPayload: String =
    s"""{"testField": true}"""

  private val testKeySchema: Schema = SchemaBuilder.record("TestRecord")
    .fields().requiredString("id").endRecord()

  private val testValSchema: Schema = SchemaBuilder.record("TestRecord")
    .fields().requiredBoolean("testField").endRecord()

  private val testValSchemaForV1: Schema = SchemaBuilder.record("TestRecord")
    .fields().name("testTimestamp").`type`(LogicalTypes.timestampMillis.addToSchema(Schema.create(Schema.Type.LONG)))
    .noDefault
    .endRecord

  implicit val guavaCache: Cache[SchemaWrapper] = GuavaCache[SchemaWrapper]

  private def ingest(request: V2IngestRequest, altValueSchema: Option[Schema] = None,
                     altSubject: Option[Subject] = None,
                     createdDate: Instant = Instant.now,
                     timestampValidationCutoffDate: Instant = timestampValidationCutoffDate): IO[KafkaClientAlgebra[IO]] = for {
    schemaRegistry <- SchemaRegistry.test[IO]
    _ <- schemaRegistry.registerSchema(altSubject.getOrElse(testSubject.value) + "-key", testKeySchema)
    _ <- schemaRegistry.registerSchema(altSubject.getOrElse(testSubject.value) + "-value", altValueSchema.getOrElse(testValSchema))
    kafkaClient <- KafkaClientAlgebra.test[IO]
    m <- TestMetadataAlgebra()
    _ <- TopicUtils.updateTopicMetadata(List(altSubject.getOrElse(testSubject.value).toString), m, createdDate)
    ingestFlow <- IO(new IngestionFlowV2[IO](schemaRegistry, kafkaClient, "https://schemaRegistry.notreal", m, timestampValidationCutoffDate))
    _ <- ingestFlow.ingest(request, altSubject.getOrElse(testSubject))
  } yield kafkaClient

  it should "ingest a record" in {
    val testRequest = V2IngestRequest(testKeyPayload, testValPayload.some, ValidationStrategy.Strict.some, useSimpleJsonFormat = false)
    ingest(testRequest).flatMap { kafkaClient =>
      kafkaClient.consumeMessages(testSubject.value, "test-consumer", commitOffsets = false).take(1).compile.toList.map { publishedMessages =>
        val firstMessage = publishedMessages.head
        (firstMessage._1.toString, firstMessage._2.get.toString) shouldBe (testKeyPayload, testValPayload)
      }
    }.unsafeRunSync()
  }

  it should "ingest a record with the simple format" in {
    val testRequest = V2IngestRequest(testKeyPayload, testValPayload.some, ValidationStrategy.Strict.some, useSimpleJsonFormat = true)
    ingest(testRequest).flatMap { kafkaClient =>
      kafkaClient.consumeMessages(testSubject.value, "test-consumer", commitOffsets = false).take(1).compile.toList.map { publishedMessages =>
        val firstMessage = publishedMessages.head
        (firstMessage._1.toString, firstMessage._2.get.toString) shouldBe (testKeyPayload, testValPayload)
      }
    }.unsafeRunSync()
  }

  it should "ingest a record with a correlationId" in {
    val headers = Headers.fromSeq(List(Header.apply("ps-correlation-id","somethinghere1234")))
    val testRequest = V2IngestRequest(testKeyPayload, testValPayload.some, ValidationStrategy.Strict.some, useSimpleJsonFormat = false,
      Some(headers))
    ingest(testRequest).flatMap { kafkaClient =>
      kafkaClient.consumeMessages(testSubject.value, "test-consumer", commitOffsets = false).take(1).compile.toList.map { publishedMessages =>
        val firstMessage = publishedMessages.head
        (firstMessage._1.toString, firstMessage._2.get.toString, firstMessage._3.get.toString) shouldBe (testKeyPayload, testValPayload, headers.toString)
      }
    }.unsafeRunSync()
  }

  it should "ingest a tombstone record" in {
    val testRequest = V2IngestRequest(testKeyPayload, None, ValidationStrategy.Strict.some, useSimpleJsonFormat = false)
    ingest(testRequest).flatMap { kafkaClient =>
      kafkaClient.consumeMessages(testSubject.value, "test-consumer", commitOffsets = false).take(1).compile.toList.map { publishedMessages =>
        val firstMessage = publishedMessages.head
        (firstMessage._1.toString, firstMessage._2) shouldBe (testKeyPayload, None)
      }
    }.unsafeRunSync()
  }

  it should "ingest a record with extra fields and Relaxed validation" in {
    val testKeyPayloadAlt: String = """{"id": "testing", "random": "blah"}"""
    val testValPayloadAlt: String =s"""{"testField": true, "other": 1000}"""

    val testRequest = V2IngestRequest(testKeyPayloadAlt, testValPayloadAlt.some, ValidationStrategy.Relaxed.some, useSimpleJsonFormat = false)
    ingest(testRequest).flatMap { kafkaClient =>
      kafkaClient.consumeMessages(testSubject.value, "test-consumer", commitOffsets = false).take(1).compile.toList.map { publishedMessages =>
        val firstMessage = publishedMessages.head
        (firstMessage._1.toString, firstMessage._2.get.toString) shouldBe (testKeyPayload, testValPayload)
      }
    }.unsafeRunSync()
  }

  it should "reject a record with extra fields and Strict validation" in {
    val testKeyPayloadAlt: String = """{"id": "testing", "random": "blah"}"""
    val testValPayloadAlt: String =s"""{"testField": true, "other": 1000}"""

    val testRequest = V2IngestRequest(testKeyPayloadAlt, testValPayloadAlt.some, ValidationStrategy.Strict.some, useSimpleJsonFormat = false)
    ingest(testRequest).attempt.unsafeRunSync() shouldBe a[Left[_, _]]
  }

  it should "reject a record with extra fields and no validation specified" in {
    val testKeyPayloadAlt: String = """{"id": "testing", "random": "blah"}"""
    val testValPayloadAlt: String =s"""{"testField": true, "other": 1000}"""

    val testRequest = V2IngestRequest(testKeyPayloadAlt, testValPayloadAlt.some, None, useSimpleJsonFormat = false)
    ingest(testRequest).attempt.unsafeRunSync() shouldBe a[Left[_, _]]
  }

  it should "reject a record that doesn't match schema" in {
    val testKeyPayloadAlt: String = """{"id": "testing"}"""
    val testValPayloadAlt: String =s"""{"testFieldOther": 1000}"""

    val testRequest = V2IngestRequest(testKeyPayloadAlt, testValPayloadAlt.some, ValidationStrategy.Strict.some, useSimpleJsonFormat = false)
    ingest(testRequest).attempt.unsafeRunSync() shouldBe a[Left[_, _]]
  }

  it should "reject payload with differing value of same name in key and value" in {
    val key = new GenericRecordBuilder(SchemaBuilder.record("key").fields().requiredString("id").endRecord())
      .set("id","12345")
      .build()
    val value = new GenericRecordBuilder(SchemaBuilder.record("value").fields().requiredString("id").endRecord())
      .set("id","54321")
      .build()
    IngestionFlowV2.validateKeyAndValueSchemas(key, value.some) match {
      case Left(error) =>
        error shouldBe KeyAndValueMismatchedValuesException(KeyAndValueMismatch("id","12345","54321"):: Nil)
      case _ => fail("Failed to properly validate key and value")
    }
  }

  it should "accept payload with same value of same name in key and value" in {
    val key = new GenericRecordBuilder(SchemaBuilder.record("key").fields().requiredString("id").endRecord())
      .set("id","12345")
      .build()
    val value = new GenericRecordBuilder(SchemaBuilder.record("value").fields().requiredString("id").endRecord())
      .set("id","12345")
      .build()
    IngestionFlowV2.validateKeyAndValueSchemas(key, value.some) shouldBe a[Right[Throwable,Unit]]
  }

  it should "accept payload with unique fields in key and value" in {
    val key = new GenericRecordBuilder(SchemaBuilder.record("key").fields().requiredString("id").endRecord())
      .set("id","12345")
      .build()
    val value = new GenericRecordBuilder(SchemaBuilder.record("value").fields().requiredString("notId").endRecord())
      .set("notId","12345")
      .build()
    IngestionFlowV2.validateKeyAndValueSchemas(key, value.some) shouldBe a[Right[Throwable,Unit]]
  }

  it should "accept payload with null value" in {
    val key = new GenericRecordBuilder(SchemaBuilder.record("key").fields().requiredString("id").endRecord())
      .set("id","12345")
      .build()
    IngestionFlowV2.validateKeyAndValueSchemas(key, None) shouldBe a[Right[Throwable,Unit]]
  }

  it should "accept a logical field type of timestamp-millis having a value 0 before timestamp-millis validation cut-off date" in {
    val testValPayloadV1: String = s"""{"testTimestamp": 0}"""
    val testRequest = V2IngestRequest(testKeyPayload, testValPayloadV1.some, ValidationStrategy.Strict.some, useSimpleJsonFormat = false)

    ingest(testRequest, testValSchemaForV1.some, testSubjectV1.some, createdDate = preTimestampValidationCutoffDate).flatMap { kafkaClient =>
      kafkaClient.consumeMessages(testSubjectV1.value, "test-consumer", commitOffsets = false).take(1).compile.toList.map { publishedMessages =>
        val firstMessage = publishedMessages.head
        (firstMessage._1.toString, firstMessage._2.get.toString) shouldBe(testKeyPayload, testValPayloadV1)
      }
    }.unsafeRunSync()
  }

  it should "accept a logical field type of timestamp-millis having a negative value -2 before timestamp-millis validation cut-off date" in {
    val testValPayloadV1: String = s"""{"testTimestamp": -2}"""
    val testRequest = V2IngestRequest(testKeyPayload, testValPayloadV1.some, ValidationStrategy.Strict.some, useSimpleJsonFormat = false)

    ingest(testRequest, testValSchemaForV1.some, testSubjectV1.some, createdDate = preTimestampValidationCutoffDate).flatMap { kafkaClient =>
      kafkaClient.consumeMessages(testSubjectV1.value, "test-consumer", commitOffsets = false).take(1).compile.toList.map { publishedMessages =>
        val firstMessage = publishedMessages.head
        (firstMessage._1.toString, firstMessage._2.get.toString) shouldBe(testKeyPayload, testValPayloadV1)
      }
    }.unsafeRunSync()
  }

  it should "throw an AvroConversionAugmentedException if a logical type(timestamp-millis) field is having a value 0 after timestamp-millis validation " +
    "cut-off date" in {
    val testValPayloadV1: String = s"""{"testTimestamp": 0}"""
    val testRequest = V2IngestRequest(testKeyPayload, testValPayloadV1.some, ValidationStrategy.Strict.some, useSimpleJsonFormat = false)
    the[AvroConversionAugmentedException] thrownBy ingest(testRequest, createdDate = postTimestampValidationCutoffDate).unsafeRunSync()
  }

  it should "throw an AvroConversionAugmentedException if a logical type(timestamp-millis) field is having a negative value -2 after timestamp-millis " +
    "validation cut-off date" in {
    val testValPayloadV1: String = s"""{"testTimestamp": -2}"""
    val testRequest = V2IngestRequest(testKeyPayload, testValPayloadV1.some, ValidationStrategy.Strict.some, useSimpleJsonFormat = false)
    the[AvroConversionAugmentedException] thrownBy ingest(testRequest, createdDate = postTimestampValidationCutoffDate).unsafeRunSync()
  }

  it should "accept a logical field type of timestamp-millis having a valid value 123 after validation cut-off date" in {
    val testValPayloadV1: String = s"""{"testTimestamp": 123}"""
    val testRequest = V2IngestRequest(testKeyPayload, testValPayloadV1.some, ValidationStrategy.Strict.some, useSimpleJsonFormat = false)

    ingest(testRequest, testValSchemaForV1.some, testSubjectV1.some,
      createdDate = postTimestampValidationCutoffDate).flatMap { kafkaClient =>
      kafkaClient.consumeMessages(testSubjectV1.value, "test-consumer", commitOffsets = false).take(1).compile.toList.map { publishedMessages =>
        val firstMessage = publishedMessages.head
        (firstMessage._1.toString, firstMessage._2.get.toString) shouldBe(testKeyPayload, testValPayloadV1)
      }
    }.unsafeRunSync()
  }

}
