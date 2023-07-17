package hydra.kafka.endpoints

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, MediaTypes, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.data.NonEmptyList
import cats.effect.{Concurrent, ContextShift, IO, Timer}
import hydra.avro.registry.SchemaRegistry
import hydra.avro.registry.SchemaRegistry.{SchemaId, SchemaVersion}
import hydra.common.NotificationsTestSuite
import hydra.common.alerting.sender.InternalNotificationSender
import hydra.core.http.CorsSupport
import hydra.core.http.security.entity.AwsConfig
import hydra.core.http.security.{AccessControlService, AwsSecurityService}
import hydra.kafka.algebras.RetryableFs2Stream.RetryPolicy.Once
import hydra.kafka.algebras._
import hydra.kafka.model.ContactMethod.{Email, Slack}
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model._
import hydra.kafka.programs.CreateTopicProgram
import hydra.kafka.serializers.TopicMetadataV2Parser
import hydra.kafka.serializers.TopicMetadataV2Parser._
import hydra.kafka.util.KafkaUtils.TopicDetails
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.scalamock.scalatest.{AsyncMockFactory, MockFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import retry.{RetryPolicies, RetryPolicy}
import spray.json._

import java.time.Instant
import scala.concurrent.ExecutionContext

final class BootstrapEndpointV2Spec
    extends AnyWordSpecLike
    with ScalatestRouteTest
    with Matchers
    with MockFactory
    with NotificationsTestSuite {

  private implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)
  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val concurrentEffect: Concurrent[IO] = IO.ioConcurrentEffect
  private implicit val logger: Logger[IO] = Slf4jLogger.getLogger
  private implicit val corsSupport: CorsSupport = new CorsSupport("http://*.vnerd.com")
  private val awsSecurityService = mock[AwsSecurityService[IO]]
  private val auth = new AccessControlService[IO](awsSecurityService, AwsConfig(Some("somecluster"), isAwsIamSecurityEnabled = false))

  private def getTestCreateTopicProgram(
      s: SchemaRegistry[IO],
      ka: KafkaAdminAlgebra[IO],
      kc: KafkaClientAlgebra[IO],
      m: MetadataAlgebra[IO],
      t: TagsAlgebra[IO]
  ): BootstrapEndpointV2[IO] = {
    val retryPolicy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
    new BootstrapEndpointV2(
      CreateTopicProgram.make[IO](
        s,
        ka,
        kc,
        retryPolicy,
        Subject.createValidated("dvs.hello-world").get,
        m
      ),
      TopicDetails(1, 1, 1),
      t,
      auth,
      awsSecurityService
    )
  }

  private val testCreateTopicProgram: IO[BootstrapEndpointV2[IO]] = {
    implicit val notificationSenderMock: InternalNotificationSender[IO] = getInternalNotificationSenderMock[IO]
    for {
      s <- SchemaRegistry.test[IO]
      k <- KafkaAdminAlgebra.test[IO]()
      kc <- KafkaClientAlgebra.test[IO]
      m <- MetadataAlgebra.make(Subject.createValidated("_metadata.topic.name").get, "bootstrap.consumer.group", kc, s, true, Once)
      t <- TagsAlgebra.make[IO]("_hydra.tags-topic","_hydra.tags-consumer", kc)
      _ <- t.createOrUpdateTag(HydraTag("DVS tag", "DVS"))
    } yield getTestCreateTopicProgram(s, k, kc, m, t)
  }

  "BootstrapEndpointV2" must {

    "reject an empty request" in {
      testCreateTopicProgram
        .map { bootstrapEndpoint =>
          Put("/v2/topics/dvs.testing") ~> Route.seal(bootstrapEndpoint.route) ~> check {
            response.status shouldBe StatusCodes.BadRequest
          }
        }
        .unsafeRunSync()
    }

    val getTestSchema: String => Schema = schemaName =>
      SchemaBuilder
        .record(schemaName)
        .fields()
        .name("test")
        .doc("text")
        .`type`()
        .stringType()
        .noDefault()
        .name(RequiredField.CREATED_AT)
        .doc("text")
        .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .withDefault(Instant.now().toEpochMilli)
        .name(RequiredField.UPDATED_AT)
        .doc("text")
        .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .withDefault(Instant.now().toEpochMilli)
        .endRecord()

    val badKeySchema: Schema =
    SchemaBuilder
      .record("key")
      .fields()
      .endRecord()

    val badKeySchemaRequest = TopicMetadataV2Request(
      Schemas(badKeySchema, getTestSchema("value")),
      StreamTypeV2.Entity,
      deprecated = false,
      None,
      Public,
      NonEmptyList.of(Email.create("test@pluralsight.com").get, Slack.create("#dev-data-platform").get),
      Instant.now,
      List.empty,
      None,
      Some("dvs-teamName"),
      None,
      List.empty,
      Some("notificationUrl")
    ).toJson.compactPrint

    val validRequestWithoutDVSTag = TopicMetadataV2Request(
      Schemas(getTestSchema("key"), getTestSchema("value")),
      StreamTypeV2.Entity,
      deprecated = false,
      None,
      Public,
      NonEmptyList.of(Email.create("test@pluralsight.com").get, Slack.create("#dev-data-platform").get),
      Instant.now,
      List.empty,
      None,
      Some("dvs-teamName"),
      None,
      List.empty,
      Some("notificationUrl")
    ).toJson.compactPrint

    val validRequestWithDVSTag = TopicMetadataV2Request(
      Schemas(getTestSchema("key"), getTestSchema("value")),
      StreamTypeV2.Entity,
      deprecated = false,
      None,
      Public,
      NonEmptyList.of(Email.create("test@pluralsight.com").get, Slack.create("#dev-data-platform").get),
      Instant.now,
      List.empty,
      None,
      Some("dvs-teamName"),
      None,
      List("DVS"),
      Some("notificationUrl")
    ).toJson.compactPrint

    "accept a valid request without a DVS tag" in {
      testCreateTopicProgram
        .map { bootstrapEndpoint =>
          Put("/v2/topics/dvs.testing", HttpEntity(ContentTypes.`application/json`, validRequestWithoutDVSTag)) ~> Route.seal(
            bootstrapEndpoint.route
          ) ~> check {
            val responseReturned = responseAs[String]
            response.status shouldBe StatusCodes.OK
          }
        }
        .unsafeRunSync()
    }

    "accept a valid request with a DVS tag" in {
      testCreateTopicProgram
        .map { bootstrapEndpoint =>
          Put("/v2/topics/dvs.testing", HttpEntity(ContentTypes.`application/json`, validRequestWithDVSTag)) ~> Route.seal(
            bootstrapEndpoint.route
          ) ~> check {
            val responseReturned = responseAs[String]
            response.status shouldBe StatusCodes.OK
          }
        }
        .unsafeRunSync()
    }

    "reject a request with invalid name" in {
      testCreateTopicProgram
        .map { bootstrapEndpoint =>
          Put("/v2/topics/invalid%20name", HttpEntity(ContentTypes.`application/json`, validRequestWithoutDVSTag)) ~> Route.seal(
            bootstrapEndpoint.route
          ) ~> check {
            val r = responseAs[String]
            r shouldBe Subject.invalidFormat
            response.status shouldBe StatusCodes.BadRequest
          }
        }
        .unsafeRunSync()
    }

    "reject a request with key missing field" in {
      testCreateTopicProgram
        .map { bootstrapEndpoint =>
          Put("/v2/topics/dvs.testing", HttpEntity(ContentTypes.`application/json`, badKeySchemaRequest)) ~> Route.seal(
            bootstrapEndpoint.route
          ) ~> check {
            val r = responseAs[String]
            response.status shouldBe StatusCodes.BadRequest
          }
        }
        .unsafeRunSync()
    }

    "reject a request without a team name" in {
      val noTeamName = TopicMetadataV2Request(
        Schemas(getTestSchema("key"), getTestSchema("value")),
        StreamTypeV2.Entity,
        deprecated = false,
        None,
        Public,
        NonEmptyList.of(Email.create("test@pluralsight.com").get, Slack.create("#dev-data-platform").get),
        Instant.now,
        List.empty,
        None,
        None,
        None,
        List.empty,
        Some("notificationUrl")
      ).toJson.compactPrint
      testCreateTopicProgram
        .map { bootstrapEndpoint =>
          Put("/v2/topics/dvs.testing", HttpEntity(ContentTypes.`application/json`, noTeamName)) ~> Route.seal(
            bootstrapEndpoint.route
          ) ~> check {
            val r = responseAs[String]
            response.status shouldBe StatusCodes.BadRequest
          }
        }
        .unsafeRunSync()
    }

    "return an InternalServerError on an unexpected exception" in {
      val failingSchemaRegistry: SchemaRegistry[IO] = new SchemaRegistry[IO] {
        private val err = IO.raiseError(new Exception)
        override def registerSchema(
            subject: String,
            schema: Schema
        ): IO[SchemaId] = err
        override def deleteSchemaOfVersion(
            subject: String,
            version: SchemaVersion
        ): IO[Unit] = err
        override def getVersion(
            subject: String,
            schema: Schema
        ): IO[SchemaVersion] = err
        override def getAllVersions(subject: String): IO[List[Int]] = err
        override def getAllSubjects: IO[List[String]] = err

        override def getSchemaRegistryClient: IO[SchemaRegistryClient] = err

        override def getLatestSchemaBySubject(subject: String): IO[Option[Schema]] = err

        override def getSchemaFor(subject: String, schemaVersion: SchemaVersion): IO[Option[Schema]] = err

        override def deleteSchemaSubject(subject: String): IO[Unit] = err
      }

      implicit val notificationSenderMock: InternalNotificationSender[IO] = getInternalNotificationSenderMock[IO]
      KafkaClientAlgebra.test[IO].flatMap { client =>
        MetadataAlgebra.make(Subject.createValidated("_metadata.topic.123.name").get, "456", client, failingSchemaRegistry, true, Once).flatMap { m =>
          KafkaAdminAlgebra
            .test[IO]()
            .map { kafka =>
              val kca = KafkaClientAlgebra.test[IO].unsafeRunSync()
              val ta = TagsAlgebra.make[IO]("_hydra.tags.topic", "_hydra.client",kca).unsafeRunSync()
              Put("/v2/topics/dvs.testing/", HttpEntity(MediaTypes.`application/json`, validRequestWithoutDVSTag)) ~> Route.seal(
                getTestCreateTopicProgram(failingSchemaRegistry, kafka, client, m, ta).route
              ) ~> check {
                response.status shouldBe StatusCodes.InternalServerError
              }
            }
        }
      }.unsafeRunSync()
    }

    "accept a request with valid tags" in {
      val validRequest = TopicMetadataV2Request(
        Schemas(getTestSchema("key"), getTestSchema("value")),
        StreamTypeV2.Entity,
        deprecated = false,
        None,
        Public,
        NonEmptyList.of(Email.create("test@pluralsight.com").get, Slack.create("#dev-data-platform").get),
        Instant.now,
        List.empty,
        None,
        Some("dvs-teamName"),
        None,
        List("DVS"),
        Some("notificationUrl")
      ).toJson.compactPrint


      testCreateTopicProgram.map {
        boostrapEndpoint =>
          Put("/v2/topics/dvs.testing", HttpEntity(ContentTypes.`application/json`, validRequest)) ~> Route.seal(
            boostrapEndpoint.route) ~> check {
            println(responseAs[String])
            response.status shouldBe StatusCodes.OK
          }
      }.unsafeRunSync()
    }

    "reject a request with invalid tags" in {
      val validRequest = TopicMetadataV2Request(
        Schemas(getTestSchema("key"), getTestSchema("value")),
        StreamTypeV2.Entity,
        deprecated = false,
        None,
        Public,
        NonEmptyList.of(Email.create("test@pluralsight.com").get, Slack.create("#dev-data-platform").get),
        Instant.now,
        List.empty,
        None,
        Some("dvs-teamName"),
        None,
        List("Source: NotValid"),
        Some("notificationUrl")
      ).toJson.compactPrint

      implicit val notificationSenderMock: InternalNotificationSender[IO] = getInternalNotificationSenderMock[IO]
      val kca = KafkaClientAlgebra.test[IO].unsafeRunSync()
      val ta = TagsAlgebra.make[IO]("_hydra.tags.topic", "_hydra.client",kca).unsafeRunSync()
      ta.createOrUpdateTag(HydraTag("Source: Something", "something hydra tag"))

      testCreateTopicProgram.map {
        boostrapEndpoint =>
          Put("/v2/topics/dvs.testing", HttpEntity(ContentTypes.`application/json`, validRequest)) ~> Route.seal(
            boostrapEndpoint.route) ~> check {
            response.status shouldBe StatusCodes.BadRequest
          }
      }.unsafeRunSync()
    }
  }

}
