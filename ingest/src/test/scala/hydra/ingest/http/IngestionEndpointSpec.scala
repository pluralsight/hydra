package hydra.ingest.http

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{RawHeader, `User-Agent`}
import akka.http.scaladsl.server.{MethodRejection, MissingHeaderRejection, RequestEntityExpectedRejection, Route}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.testkit.{TestActorRef, TestKit}
import cats.effect.{Concurrent, ContextShift, IO}
import hydra.avro.registry.SchemaRegistry
import hydra.common.util.ActorUtils
import hydra.core.ingest.RequestParams
import RequestParams._
import hydra.core.marshallers.GenericError
import hydra.ingest.IngestorInfo
import hydra.ingest.services.{IngestionFlow, IngestionFlowV2}
import hydra.ingest.services.IngestorRegistry.{FindAll, FindByName, LookupResult}
import hydra.ingest.test.TestIngestor
import hydra.kafka.algebras.KafkaClientAlgebra
import org.apache.avro.SchemaBuilder
import org.joda.time.DateTime
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

final class IngestionEndpointSpec
    extends Matchers
    with AnyWordSpecLike
    with ScalatestRouteTest
    with HydraIngestJsonSupport {

  private implicit val timeout = RouteTestTimeout(10.seconds)
  val probe = system.actorOf(Props[TestIngestor])

  val ingestorInfo =
    IngestorInfo(ActorUtils.actorName(probe), "test", probe.path, DateTime.now)

  val registry = TestActorRef(
    new Actor {
      override def receive = {
        case FindByName(name) if name == "tester" =>
          sender ! LookupResult(Seq(ingestorInfo))
        case FindByName(name) if name == "error" =>
          throw new IllegalArgumentException("RAR")
        case FindByName(_) => sender ! LookupResult(Seq.empty)
        case FindAll       => sender ! LookupResult(Seq(ingestorInfo))
      }
    },
    "ingestor_registry"
  ).underlyingActor

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val concurrentEffect: Concurrent[IO] = IO.ioConcurrentEffect

  import scalacache.Mode
  implicit val mode: Mode[IO] = scalacache.CatsEffect.modes.async
  private val ingestRoute = new IngestionEndpoint(
    false,
    new IngestionFlow[IO](SchemaRegistry.test[IO].unsafeRunSync, KafkaClientAlgebra.test[IO].unsafeRunSync, "https://schemaregistryUrl.notreal"),
    new IngestionFlowV2[IO](SchemaRegistry.test[IO].unsafeRunSync, KafkaClientAlgebra.test[IO].unsafeRunSync, "https://schemaregistryUrl.notreal"),
    Set.empty
  ).route

  override def afterAll = {
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
    (for {
      schemaRegistry <- SchemaRegistry.test[IO]
      _ <- schemaRegistry.registerSchema("my_topic-value", otherSchema)
      _ <- schemaRegistry.registerSchema("my_topic-value", otherSchema)
      _ <- schemaRegistry.registerSchema("exp.blah.blah-value", simpleSchema)
      _ <- schemaRegistry.registerSchema("exp.blah.blah-key", simpleSchema)
    } yield {
      new IngestionEndpoint(
        true,
        new IngestionFlow[IO](schemaRegistry, KafkaClientAlgebra.test[IO].unsafeRunSync, "https://schemaregistry.notreal"),
        new IngestionFlowV2[IO](schemaRegistry, KafkaClientAlgebra.test[IO].unsafeRunSync, "https://schemaregistry.notreal"),
        Set("Segment"),
        Some("alt-test-request-handler")
      ).route
    }).unsafeRunSync()
  }


  "The ingestor endpoint" should {

    "rejects a GET request" in {
      Get("/ingest") ~> ingestRoute ~> check {
        rejections should contain allElementsOf (Seq(
          MethodRejection(HttpMethods.POST),
          MethodRejection(HttpMethods.DELETE)
        ))
      }
    }

    "rejects empty requests" in {
      Post("/ingest") ~> ingestRoute ~> check {
        rejection shouldEqual RequestEntityExpectedRejection
      }
    }

    "initiates a delete request" in {
      val key = RawHeader(RequestParams.HYDRA_RECORD_KEY_PARAM, "test")
      Delete("/ingest").withHeaders(key) ~> ingestRoute ~> check {
        response.status.intValue() shouldBe 200 //todo: should we support a 204?
      }
    }

    "rejects a delete request without a key" in {
      Delete("/ingest") ~> ingestRoute ~> check {
        rejection shouldEqual MissingHeaderRejection("hydra-record-key")
      }
    }

    "publishes to a target ingestor" in {
      val ingestor = RawHeader(RequestParams.HYDRA_INGESTOR_PARAM, "tester")
      val request = Post("/ingest", "payload").withHeaders(ingestor)
      request ~> ingestRoute ~> check {
        status shouldBe StatusCodes.OK
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
      val request = Post("/ingest", "payload")
      request ~> ingestRoute ~> check {
        status shouldBe StatusCodes.OK
      }
    }


    "publishes to a target ingestor for UA in provided Set" in {
      val ingestor = RawHeader(RequestParams.HYDRA_INGESTOR_PARAM, "tester")
      val userAgent = `User-Agent`("Segment.com")
      val kafkaTopic = RawHeader(HYDRA_KAFKA_TOPIC_PARAM, "my_topic")

      val request = Post("/ingest", "payload").withHeaders(ingestor, userAgent, kafkaTopic)
      request ~> ingestRouteAlt ~> check {
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
    "reject an uncomplete request" in {
      val request = Post("/v2/topics/exp.blah.blah/records", HttpEntity(ContentTypes.`application/json`, """{"test":true, "extraField":true}"""))
      request ~> Route.seal(ingestRouteAlt) ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }
    "accept a complete request" in {
      val request = Post("/v2/topics/exp.blah.blah/records", HttpEntity(ContentTypes.`application/json`, """{"key":{"test": 1}, "value":{"test": 2}}"""))
      request ~> Route.seal(ingestRouteAlt) ~> check {
        responseAs[String] shouldBe "OK"
        status shouldBe StatusCodes.OK
      }
    }
  }
}
