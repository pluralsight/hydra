package hydra.ingest.http

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{RawHeader, `User-Agent`}
import akka.http.scaladsl.server.{MethodRejection, MissingHeaderRejection, RequestEntityExpectedRejection, Route}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.TestKit
import cats.effect.{Concurrent, ContextShift, IO, Timer}
import hydra.avro.registry.SchemaRegistry
import hydra.avro.util.SchemaWrapper
import hydra.core.http.security.{AccessControlService, AwsSecurityService}
import hydra.core.http.security.entity.AwsConfig
import hydra.core.ingest.RequestParams
import hydra.core.ingest.RequestParams._
import hydra.core.marshallers.GenericError
import hydra.ingest.services.{IngestionFlow, IngestionFlowV2}
import hydra.ingest.utils.TopicUtils
import hydra.kafka.algebras.{KafkaClientAlgebra, TestMetadataAlgebra}
import hydra.kafka.model.TopicMetadataV2Request.Subject
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scalacache.Cache
import scalacache.guava.GuavaCache

import java.time.Instant
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

final class IngestionEndpointSpec
    extends Matchers
    with AnyWordSpecLike
    with ScalatestRouteTest
    with MockFactory
    with HydraIngestJsonSupport {
  implicit val logger: SelfAwareStructuredLogger[IO] =  Slf4jLogger.getLogger[IO]
  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val concurrentEffect: Concurrent[IO] = IO.ioConcurrentEffect
  implicit val guavaCache: Cache[SchemaWrapper] = GuavaCache[SchemaWrapper]

  private val noAuth = new AccessControlService[IO](mock[AwsSecurityService[IO]], AwsConfig(None, isAwsIamSecurityEnabled = false))
  private val testSubject = Subject.createValidated("dvs.test.v0.Testing").get
  private val timestampValidationCutoffDate: Instant = Instant.parse("2023-07-11T00:00:00Z")

  import scalacache.Mode
  implicit val mode: Mode[IO] = scalacache.CatsEffect.modes.async
  private val ingestRoute = {
    val simpleSchema = SchemaBuilder.record("test").fields.requiredInt("test").endRecord()
    val schemaReg =(for {
      sr <- SchemaRegistry.test[IO]
      _ <- sr.registerSchema("testtopic-value", simpleSchema)
    } yield sr).unsafeRunSync
    val metadata = (for {
      m <- TestMetadataAlgebra()
      _ <- TopicUtils.updateTopicMetadata(List(testSubject.value), m)
    } yield m).unsafeRunSync
    new IngestionEndpoint(
      new IngestionFlow[IO](schemaReg, KafkaClientAlgebra.test[IO].unsafeRunSync, "https://schemaregistryUrl.notreal"),
      new IngestionFlowV2[IO](SchemaRegistry.test[IO].unsafeRunSync, KafkaClientAlgebra.test[IO].unsafeRunSync, "https://schemaregistryUrl.notreal",
        metadata), noAuth
    ).route
  }

  override def afterAll: Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(
      system,
      verifySystemShutdown = true,
      duration = 10 seconds
    )
  }

  private val ingestRouteAlt = {
    val simpleSchema = SchemaBuilder.record("test").fields.requiredInt("test").endRecord()
    val otherSchema = SchemaBuilder.record("my_topic").fields().requiredBoolean("test").optionalInt("intField").endRecord()

    val uuidType = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING))
    val uuidSchemaKey = SchemaBuilder.record("test").fields().name("id").`type`(uuidType).noDefault().endRecord()

    val timestampsType = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))
    val timestampsSchemaKey = SchemaBuilder.record("test").fields().name("time").`type`(timestampsType).noDefault().endRecord()

    val compositeKey = SchemaBuilder.record("test").fields.requiredInt("id1").requiredInt("id2").endRecord()

    (for {
      schemaRegistry <- SchemaRegistry.test[IO]
      _ <- schemaRegistry.registerSchema("my_topic-value", otherSchema)
      _ <- schemaRegistry.registerSchema("my_topic-value", otherSchema)
      _ <- schemaRegistry.registerSchema("dvs.blah.blah-key", simpleSchema)
      _ <- schemaRegistry.registerSchema("dvs.blah.blah-value", simpleSchema)
      _ <- schemaRegistry.registerSchema("dvs.blah.uuid-key", uuidSchemaKey)
      _ <- schemaRegistry.registerSchema("dvs.blah.uuid-value", simpleSchema)
      _ <- schemaRegistry.registerSchema("dvs.blah.uuid2-key", uuidSchemaKey)
      _ <- schemaRegistry.registerSchema("dvs.blah.uuid2-value", uuidSchemaKey)
      _ <- schemaRegistry.registerSchema("dvs.blah.timestamps-key", timestampsSchemaKey)
      _ <- schemaRegistry.registerSchema("dvs.blah.timestamps-value", simpleSchema)
      _ <- schemaRegistry.registerSchema("dvs.blah.timestamps2-key", timestampsSchemaKey)
      _ <- schemaRegistry.registerSchema("dvs.blah.timestamps2-value", timestampsSchemaKey)
      _ <- schemaRegistry.registerSchema("dvs.blah.composit-key", compositeKey)
      _ <- schemaRegistry.registerSchema("dvs.blah.composit-value", simpleSchema)
      m <- TestMetadataAlgebra()
      _ <- TopicUtils.updateTopicMetadata(List(testSubject.value), m)
    } yield {
      new IngestionEndpoint(
        new IngestionFlow[IO](schemaRegistry, KafkaClientAlgebra.test[IO].unsafeRunSync, "https://schemaregistry.notreal"),
        new IngestionFlowV2[IO](schemaRegistry, KafkaClientAlgebra.test[IO].unsafeRunSync, "https://schemaregistry.notreal", m),
        noAuth
      ).route
    }).unsafeRunSync()
  }


  "The ingestor endpoint" should {

    "rejects a GET request" in {
      Get("/ingest") ~> ingestRoute ~> check {
        rejections should contain allElementsOf Seq(
          MethodRejection(HttpMethods.POST),
          MethodRejection(HttpMethods.DELETE)
        )
      }
    }

    "rejects empty requests" in {
      Post("/ingest") ~> ingestRoute ~> check {
        rejection shouldEqual RequestEntityExpectedRejection
      }
    }

    "initiates a delete request" in {
      val key = RawHeader(RequestParams.HYDRA_RECORD_KEY_PARAM, "test")
      val topic = RawHeader(RequestParams.HYDRA_KAFKA_TOPIC_PARAM, "testtopic")
      Delete("/ingest").withHeaders(key, topic) ~> ingestRoute ~> check {
        response.status.intValue() shouldBe 200
      }
    }

    "rejects a delete request without a key" in {
      Delete("/ingest") ~> ingestRoute ~> check {
        rejection shouldEqual MissingHeaderRejection("hydra-record-key")
      }
    }

    "rejects a request with an invalid ack strategy" in {
      val ingestor = RawHeader(RequestParams.HYDRA_INGESTOR_PARAM, "tester")
      val request = Post("/ingest", "payload").withHeaders(
        ingestor,
        RawHeader(RequestParams.HYDRA_ACK_STRATEGY, "invalid")
      )
      request ~> ingestRoute ~> check {
        status shouldBe StatusCodes.BadRequest
        entityAs[GenericError].status shouldBe 400
      }
    }

    "returns 404 if unknown ingestor" in {
      val ingestor = RawHeader(RequestParams.HYDRA_INGESTOR_PARAM, "unknown")
      val request = Post("/ingest", "payload").withHeaders(ingestor)
      request ~> ingestRoute ~> check {
        status shouldBe StatusCodes.NotFound
      }
    }

    "broadcasts a request" in {
      val kafkaTopic = RawHeader(HYDRA_KAFKA_TOPIC_PARAM, "testtopic")
      val request = Post("/ingest", """{"test": 2020}""").withHeaders(kafkaTopic)
      request ~> ingestRoute ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    "accepts for UA not in provided Set" in {
      val ingestor = RawHeader(RequestParams.HYDRA_INGESTOR_PARAM, "tester")
      val userAgent = `User-Agent`("not_found")
      val kafkaTopic = RawHeader(HYDRA_KAFKA_TOPIC_PARAM, "my_topic")

      val request = Post("/ingest", """{"test":true}""").withHeaders(ingestor, userAgent, kafkaTopic)
      request ~> ingestRouteAlt ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    "rejects for a bad payload" in {
      val kafkaTopic = RawHeader(HYDRA_KAFKA_TOPIC_PARAM, "my_topic")

      val request = Post("/ingest", """{}""").withHeaders(kafkaTopic)
      request ~> ingestRouteAlt ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }

    "rejects for a bad json payload" in {
      val kafkaTopic = RawHeader(HYDRA_KAFKA_TOPIC_PARAM, "my_topic")

      val request = Post("/ingest", """{"test":00.0123}""").withHeaders(kafkaTopic)
      request ~> ingestRouteAlt ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }

    "rejects for an incorrect int type in the payload" in {
      val kafkaTopic = RawHeader(HYDRA_KAFKA_TOPIC_PARAM, "my_topic")

      val request = Post("/ingest", """{"test":true, "intField":false}""").withHeaders(kafkaTopic)
      request ~> ingestRouteAlt ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }

    "rejects for an extra field when using strict validation" in {
      val kafkaTopic = RawHeader(HYDRA_KAFKA_TOPIC_PARAM, "my_topic")

      val request = Post("/ingest", """{"test":true, "extraField":true}""").withHeaders(kafkaTopic)
      request ~> ingestRouteAlt ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[String] should include("""com.pluralsight.hydra.avro.UndefinedFieldsException: Field(s) 'extraField' are not defined in the schema and validation is set to strict. Declared fields are: test,intField. [https://schemaregistry.notreal/subjects/my_topic-value/versions/latest/schema]""")
      }
    }

    "accepts for an extra field when using relaxed validation" in {
      val kafkaTopic = RawHeader(HYDRA_KAFKA_TOPIC_PARAM, "my_topic")
      val validation = RawHeader(HYDRA_VALIDATION_STRATEGY, "relaxed")

      val request = Post("/ingest", """{"test":true, "extraField":true}""").withHeaders(kafkaTopic, validation)
      request ~> ingestRouteAlt ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    "receive BadRequest for publishing to topic that does not exist" in {
      val topic = "my_topic_DNE"
      val kafkaTopic = RawHeader(HYDRA_KAFKA_TOPIC_PARAM, topic)
      val validation = RawHeader(HYDRA_VALIDATION_STRATEGY, "relaxed")

      val request = Post("/ingest", """{"test":true, "extraField":true}""").withHeaders(kafkaTopic, validation)
      request ~> ingestRouteAlt ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[String] should include(s"Schema '$topic' cannot be loaded. Cause: hydra.avro.resource.SchemaResourceLoader$$SchemaNotFoundException: Schema not found for $topic")
      }
    }
  }

  "The V2 Ingestion path" should {
    "accept a composite key" in {
      val validation = RawHeader(HYDRA_VALIDATION_STRATEGY, "relaxed")
      val request = Post("/v2/topics/dvs.blah.composit/records", HttpEntity(ContentTypes.`application/json`,
        """{
          |"hydra.key": "id1, id2",
          |"extraField":true,
          |"key":{"id1": 123, "id2": 321},
          |"value":{"test": 2}
          |}
          |""".stripMargin)).withHeaders(validation)
      request ~> Route.seal(ingestRouteAlt) ~> check {
        responseAs[String] shouldBe "{\"offset\":0,\"partition\":0}"
        status shouldBe StatusCodes.OK
      }
    }
    "unaccept a bad composite key" in {
      val validation = RawHeader(HYDRA_VALIDATION_STRATEGY, "relaxed")
      val request = Post("/v2/topics/dvs.blah.composit/records", HttpEntity(ContentTypes.`application/json`,
        """{
          |"hydra.key": "id1, id2",
          |"extraField":true,
          |"key":{"id3": 123, "id2": 321},
          |"value":{"test": 2}
          |}
          |""".stripMargin)).withHeaders(validation)
      request ~> Route.seal(ingestRouteAlt) ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }
    "accept a valid UUID logical type in a key" in {
      val request = Post("/v2/topics/dvs.blah.uuid/records", HttpEntity(ContentTypes.`application/json`, """{"key":{"id": "e1917f98-049f-448f-bc63-6184b46dbfa7"}, "value":{"test": 2}}"""))
      request ~> Route.seal(ingestRouteAlt) ~> check {
        responseAs[String] shouldBe "{\"offset\":0,\"partition\":0}"
        status shouldBe StatusCodes.OK
      }
    }
    "reject an invalid UUID logical type in a key" in {
      val request = Post("/v2/topics/dvs.blah.uuid/records", HttpEntity(ContentTypes.`application/json`, """{"key":{"id": "6184b46dbfa7"}, "value":{"test": 2}}"""))
      request ~> Route.seal(ingestRouteAlt) ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }
    "accept a valid UUID logical type in a value" in {
      val request = Post("/v2/topics/dvs.blah.uuid2/records", HttpEntity(ContentTypes.`application/json`, """{"key":{"id": "e1917f98-049f-448f-bc63-6184b46dbfa7"}, "value":{"id": "e1917f98-049f-448f-bc63-6184b46dbfa7"}}"""))
      request ~> Route.seal(ingestRouteAlt) ~> check {
        responseAs[String] shouldBe "{\"offset\":0,\"partition\":0}"
        status shouldBe StatusCodes.OK
      }
    }
    "reject an invalid UUID logical type in a value" in {
      val request = Post("/v2/topics/dvs.blah.uuid2/records", HttpEntity(ContentTypes.`application/json`, """{"key":{"id": "e1917f98-049f-448f-bc63-6184b46dbfa7"}, "value":{"id": "e1917f98"}}"""))
      request ~> Route.seal(ingestRouteAlt) ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }
    "accept a valid timestamps logical type in a key" in {
      val request = Post("/v2/topics/dvs.blah.timestamps/records", HttpEntity(ContentTypes.`application/json`, """{"key":{"time": 1649071249966}, "value":{"test": 2}}"""))
      request ~> Route.seal(ingestRouteAlt) ~> check {
        responseAs[String] shouldBe "{\"offset\":0,\"partition\":0}"
        status shouldBe StatusCodes.OK
      }
    }
    "reject an invalid timestamps logical type in a key" in {
      val request = Post("/v2/topics/dvs.blah.timestamps/records", HttpEntity(ContentTypes.`application/json`, """{"key":{"time": "1649071249966"}, "value":{"test": 2}}"""))
      request ~> Route.seal(ingestRouteAlt) ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }
    "accept a valid timestamps logical type in a value" in {
      val request = Post("/v2/topics/dvs.blah.timestamps2/records", HttpEntity(ContentTypes.`application/json`, """{"key":{"time": 1649071249966}, "value":{"time": 1649071249966}}"""))
      request ~> Route.seal(ingestRouteAlt) ~> check {
        responseAs[String] shouldBe "{\"offset\":0,\"partition\":0}"
        status shouldBe StatusCodes.OK
      }
    }
    "reject an invalid timestamps logical type in a value" in {
      val request = Post("/v2/topics/dvs.blah.timestamps2/records", HttpEntity(ContentTypes.`application/json`, """{"key":{"time": 1649071249966}, "value":{"time": "1649071249966"}}"""))
      request ~> Route.seal(ingestRouteAlt) ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }
    "reject an uncomplete request" in {
      val request = Post("/v2/topics/dvs.blah.blah/records", HttpEntity(ContentTypes.`application/json`, """{"test":true, "extraField":true}"""))
      request ~> Route.seal(ingestRouteAlt) ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }
    "reject an request without information in the key" in {
      val request = Post("/v2/topics/dvs.blah.blah/records", HttpEntity(ContentTypes.`application/json`, """{"key":{}, "value":{"test": 2}}"""))
      request ~> Route.seal(ingestRouteAlt) ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }
    "accept a complete request" in {
      val request = Post("/v2/topics/dvs.blah.blah/records", HttpEntity(ContentTypes.`application/json`, """{"key":{"test": 1}, "value":{"test": 1}}"""))
      request ~> Route.seal(ingestRouteAlt) ~> check {
        responseAs[String] shouldBe "{\"offset\":0,\"partition\":0}"
        status shouldBe StatusCodes.OK
      }
    }
    "accept a request with correlationId header" in {
      val correlationId = RawHeader("ps-correlation-id", "th1s1ss0m3c0RR3l4t10n")
      val request = Post("/v2/topics/dvs.blah.blah/records", HttpEntity(ContentTypes.`application/json`, """{"key":{"test": 1}, "value":{"test": 1}}""")).withHeaders(correlationId)
      request ~> Route.seal(ingestRouteAlt) ~> check {
        responseAs[String] shouldBe "{\"offset\":1,\"partition\":0}"
        status shouldBe StatusCodes.OK
      }
    }
  }
}
