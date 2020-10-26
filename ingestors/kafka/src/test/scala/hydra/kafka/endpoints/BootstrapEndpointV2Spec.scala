package hydra.kafka.endpoints

import java.time.Instant

import akka.http.javadsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentType, ContentTypes, HttpEntity, HttpHeader, MediaTypes, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.data.NonEmptyList
import cats.effect.{Concurrent, ContextShift, IO, Timer}
import hydra.avro.registry.SchemaRegistry
import hydra.avro.registry.SchemaRegistry.{SchemaId, SchemaVersion}
import hydra.core.marshallers.{History, StreamType}
import hydra.kafka.algebras.{KafkaAdminAlgebra, KafkaClientAlgebra, MetadataAlgebra}
import hydra.kafka.model.ContactMethod.{Email, Slack}
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model._
import hydra.kafka.programs.CreateTopicProgram
import hydra.kafka.serializers.TopicMetadataV2Parser
import hydra.kafka.util.KafkaUtils.TopicDetails
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import retry.{RetryPolicies, RetryPolicy}
import spray.json._
import TopicMetadataV2Parser._

import scala.concurrent.ExecutionContext

final class BootstrapEndpointV2Spec
    extends AnyWordSpecLike
    with ScalatestRouteTest
    with Matchers {

  private implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)
  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val concurrentEffect: Concurrent[IO] = IO.ioConcurrentEffect
  private implicit val logger: Logger[IO] = Slf4jLogger.getLogger

  private def getTestCreateTopicProgram(
      s: SchemaRegistry[IO],
      ka: KafkaAdminAlgebra[IO],
      kc: KafkaClientAlgebra[IO],
      m: MetadataAlgebra[IO]
  ): BootstrapEndpointV2[IO] = {
    val retryPolicy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
    new BootstrapEndpointV2(
      new CreateTopicProgram[IO](
        s,
        ka,
        kc,
        retryPolicy,
        Subject.createValidated("dvs.hello-world").get,
        m
      ),
      TopicDetails(1, 1)
    )
  }

  private val testCreateTopicProgram: IO[BootstrapEndpointV2[IO]] =
    for {
      s <- SchemaRegistry.test[IO]
      k <- KafkaAdminAlgebra.test[IO]
      kc <- KafkaClientAlgebra.test[IO]
      m <- MetadataAlgebra.make("_metadata.topic.name", "bootstrap.consumer.group", kc, s, true)
    } yield getTestCreateTopicProgram(s, k, kc, m)

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
        .`type`()
        .stringType()
        .noDefault()
        .endRecord()

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
      Some("dvs-teamName")
    ).toJson.compactPrint

    "accept a valid request" in {
      testCreateTopicProgram
        .map { bootstrapEndpoint =>
          Put("/v2/topics/dvs.testing", HttpEntity(ContentTypes.`application/json`, validRequest)) ~> Route.seal(
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
          Put("/v2/topics/invalid%20name", HttpEntity(ContentTypes.`application/json`, validRequest)) ~> Route.seal(
            bootstrapEndpoint.route
          ) ~> check {
            val r = responseAs[String]
            r shouldBe Subject.invalidFormat
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
        None
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
      KafkaClientAlgebra.test[IO].flatMap { client =>
        MetadataAlgebra.make("123", "456", client, failingSchemaRegistry, true).flatMap { m =>
          KafkaAdminAlgebra
            .test[IO]
            .map { kafka =>
              Put("/v2/topics/dvs.testing/", HttpEntity(MediaTypes.`application/json`, validRequest)) ~> Route.seal(
                getTestCreateTopicProgram(failingSchemaRegistry, kafka, client, m).route
              ) ~> check {
                response.status shouldBe StatusCodes.InternalServerError
              }
            }
        }
      }.unsafeRunSync()
    }
  }

}
