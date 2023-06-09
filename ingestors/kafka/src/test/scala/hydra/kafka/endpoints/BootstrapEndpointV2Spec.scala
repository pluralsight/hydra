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
import hydra.kafka.algebras.RetryableFs2Stream.RetryPolicy.Once
import hydra.kafka.algebras._
import hydra.kafka.model.ContactMethod.{Email, Slack}
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model._
import hydra.kafka.programs.CreateTopicProgram
import hydra.kafka.programs.KeyAndValueSchemaV2Validator.DEFAULT_LOOPHOLE_CUTOFF_DATE_DEFAULT_VALUE
import hydra.kafka.serializers.TopicMetadataV2Parser._
import hydra.kafka.util.GenericUtils
import hydra.kafka.util.KafkaUtils.TopicDetails
import hydra.kafka.utils.FakeV2TopicMetadata
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.avro.SchemaBuilder.{FieldAssembler, GenericDefault}
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import retry.{RetryPolicies, RetryPolicy}
import spray.json._

import java.time.Instant
import scala.concurrent.ExecutionContext
import scala.language.implicitConversions

final class BootstrapEndpointV2Spec
    extends AnyWordSpecLike
    with ScalatestRouteTest
    with Matchers
    with NotificationsTestSuite {

  private implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)
  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val concurrentEffect: Concurrent[IO] = IO.ioConcurrentEffect
  private implicit val logger: Logger[IO] = Slf4jLogger.getLogger
  private implicit val corsSupport: CorsSupport = new CorsSupport("http://*.vnerd.com")

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
      t
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

  private def testCreatedTopicProgram(topics: List[String], createdDate: Instant): IO[BootstrapEndpointV2[IO]] = {
    implicit val notificationSenderMock: InternalNotificationSender[IO] = getInternalNotificationSenderMock[IO]
    for {
      s <- SchemaRegistry.test[IO]
      k <- KafkaAdminAlgebra.test[IO]()
      kc <- KafkaClientAlgebra.test[IO]
      m <- TestMetadataAlgebra()
      _ <- FakeV2TopicMetadata.writeV2TopicMetadata(topics, m, Option(createdDate))
      t <- TagsAlgebra.make[IO]("_hydra.tags-topic", "_hydra.tags-consumer", kc)
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

    implicit def addOptionalDefaultValue[R](gd: GenericDefault[R]): CustomGenericDefault[R] = new CustomGenericDefault[R](gd)

    def getTestSchema(s: String,
                      createdAtDefaultValue: Option[Long] = None,
                      updatedAtDefaultValue: Option[Long] = None): Schema =
      SchemaBuilder
        .record(s)
        .fields()
        .name("test")
        .doc("text")
        .`type`()
        .stringType()
        .noDefault()
        .name(RequiredField.CREATED_AT)
        .doc("text")
        .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .default(createdAtDefaultValue)
        .name(RequiredField.UPDATED_AT)
        .doc("text")
        .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .default(updatedAtDefaultValue)
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

    def getTopicMetadataV2Request(valueSchema: Schema) = {
      TopicMetadataV2Request(
        Schemas(getTestSchema("key"), valueSchema),
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
    }

    val validRequestWithoutDVSTag = getTopicMetadataV2Request(getTestSchema("value"))

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

    Seq(
      "20230101" -> "Pre cutoff date",
      DEFAULT_LOOPHOLE_CUTOFF_DATE_DEFAULT_VALUE -> "On cutoff date"
    ) foreach {
      case (date, dateDescription) =>
        val dateInstant = GenericUtils.dateStringToInstant(date)
        val dateInMillis = Option(dateInstant.toEpochMilli)
        Seq(
          getTestSchema("value", createdAtDefaultValue = dateInMillis) -> Seq("createdAt"),
          getTestSchema("value", updatedAtDefaultValue = dateInMillis) -> Seq("updatedAt"),
          getTestSchema("value", createdAtDefaultValue = dateInMillis, updatedAtDefaultValue = dateInMillis) -> Seq("createdAt", "updatedAt")
        ) foreach {
          case (valueSchema, fields) =>
            s"[$dateDescription] For existing topic, accept a request with mandatory fields having a default value - ${fields.mkString(", ")}" in {
              testCreatedTopicProgram(topics = List("dvs.testing"), dateInstant)
                .map { bootstrapEndpoint =>
                  Put("/v2/topics/dvs.testing", HttpEntity(ContentTypes.`application/json`,
                    getTopicMetadataV2Request(valueSchema))
                  ) ~> Route.seal(bootstrapEndpoint.route) ~> check {
                    response.status shouldBe StatusCodes.OK
                  }
                }
                .unsafeRunSync()
            }
        }
    }

    Seq(
      getTestSchema("value", createdAtDefaultValue = Option(Instant.now().toEpochMilli)) -> Seq("createdAt"),
      getTestSchema("value", updatedAtDefaultValue = Option(Instant.now().toEpochMilli)) -> Seq("updatedAt"),
      getTestSchema("value", createdAtDefaultValue = Option(Instant.now().toEpochMilli),
        updatedAtDefaultValue = Option(Instant.now().toEpochMilli)) -> Seq("createdAt", "updatedAt")
    ) foreach {
      case (valueSchema, fields) =>
        s"[Post cutoff date] reject a request with mandatory fields having a default value - ${fields.mkString(", ")}" in {
          testCreateTopicProgram
            .map { bootstrapEndpoint =>
              Put("/v2/topics/dvs.testing", HttpEntity(ContentTypes.`application/json`, getTopicMetadataV2Request(valueSchema))) ~> Route.seal(
                bootstrapEndpoint.route
              ) ~> check {
                val errorStringSeq = fields map { f =>
                  s"""Required field cannot have a default value in the value schema fields: field name = $f, value schema = $valueSchema, stream type = Entity."""
                }

                response.status shouldBe StatusCodes.BadRequest
                responseAs[String] shouldBe s"${errorStringSeq.mkString("\n")}"
              }
            }
            .unsafeRunSync()
      }
    }
  }

  class CustomGenericDefault[R](gd: GenericDefault[R]) {
    def default(defaultValue: Option[Long]): FieldAssembler[R] = defaultValue match {
      case Some(dv) => gd.withDefault(dv)
      case None => gd.noDefault()
    }
  }
}
