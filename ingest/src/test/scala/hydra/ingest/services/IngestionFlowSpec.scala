package hydra.ingest.services

import cats.effect.{Concurrent, ContextShift, IO}
import hydra.avro.registry.SchemaRegistry
import hydra.core.ingest.HydraRequest
import hydra.core.ingest.RequestParams.{HYDRA_KAFKA_TOPIC_PARAM,HYDRA_RECORD_KEY_PARAM}
import hydra.ingest.services.IngestionFlow.MissingTopicNameException
import hydra.kafka.algebras.KafkaClientAlgebra
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class IngestionFlowSpec extends AnyFlatSpec with Matchers {

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val concurrentEffect: Concurrent[IO] = IO.ioConcurrentEffect
  private implicit val mode: scalacache.Mode[IO] = scalacache.CatsEffect.modes.async

  private val testSubject: String = "test_subject"

  private val testSubjectNoKey: String = "test_subject_no_key"

  private val testKey: String = "test"

  private val testPayload: String =
    s"""{"id": "$testKey", "testField": true}"""

  private val testSchema: Schema = SchemaBuilder.record("TestRecord")
    .prop("hydra.key", "id")
    .fields().requiredString("id").requiredBoolean("testField").endRecord()

  private val testSchemaNoKey: Schema = SchemaBuilder.record("TestRecordNoKey")
    .fields().requiredString("id").requiredBoolean("testField").endRecord()

  private def ingest(request: HydraRequest): IO[KafkaClientAlgebra[IO]] = for {
    schemaRegistry <- SchemaRegistry.test[IO]
    _ <- schemaRegistry.registerSchema(testSubject + "-value", testSchema)
    _ <- schemaRegistry.registerSchema(testSubjectNoKey + "-value", testSchemaNoKey)
    kafkaClient <- KafkaClientAlgebra.test[IO]
    ingestFlow <- IO(new IngestionFlow[IO](schemaRegistry, kafkaClient, "https://schemaRegistry.notreal"))
    _ <- ingestFlow.ingest(request)
  } yield kafkaClient

  it should "ingest a message" in {
    val testRequest = HydraRequest("correlationId", testPayload, metadata = Map(HYDRA_KAFKA_TOPIC_PARAM -> testSubject))
    ingest(testRequest).flatMap { kafkaClient =>
      kafkaClient.consumeStringKeyMessages(testSubject, "test-consumer", commitOffsets = false).take(1).compile.toList.map { publishedMessages =>
        val firstMessage = publishedMessages.head
        (firstMessage._1, firstMessage._2.get.toString) shouldBe (Some(testKey), testPayload)
      }
    }.unsafeRunSync()
  }

  it should "ingest a message with a null key" in {
    val testRequest = HydraRequest("correlationId", testPayload, metadata = Map(HYDRA_KAFKA_TOPIC_PARAM -> testSubjectNoKey))
    ingest(testRequest).flatMap { kafkaClient =>
      kafkaClient.consumeStringKeyMessages(testSubjectNoKey, "test-consumer", commitOffsets = false).take(1).compile.toList.map { publishedMessages =>
        val firstMessage = publishedMessages.head
        (firstMessage._1, firstMessage._2.get.toString) shouldBe (None, testPayload)
      }
    }.unsafeRunSync()
  }

  it should "return an error when no topic name is provided" in {
    val testRequest = HydraRequest("correlationId", testPayload)
    ingest(testRequest).attempt.unsafeRunSync() shouldBe Left(MissingTopicNameException(testRequest))
  }

  it should "take the key from the header if present" in {
    val headerKey = "someDifferentKey"
    val testRequest = HydraRequest("correlationId", testPayload, metadata = Map(HYDRA_RECORD_KEY_PARAM -> headerKey, HYDRA_KAFKA_TOPIC_PARAM -> testSubject))
    ingest(testRequest).flatMap { kafkaClient =>
      kafkaClient.consumeStringKeyMessages(testSubject, "test-consumer", commitOffsets = false).take(1).compile.toList.map { publishedMessages =>
        val firstMessage = publishedMessages.head
        (firstMessage._1, firstMessage._2.get.toString) shouldBe (Some(headerKey), testPayload)
      }
    }.unsafeRunSync()

  }

}
