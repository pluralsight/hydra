package hydra.core.bootstrap

import cats.effect.concurrent.Ref
import cats.effect.{IO, Sync, Timer}
import cats.implicits._
import hydra.avro.registry.SchemaRegistry
import hydra.avro.registry.SchemaRegistry.{SchemaId, SchemaVersion}
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.{Matchers, WordSpec}
import retry.{RetryPolicies, RetryPolicy}

class CreateTopicSpec extends WordSpec with Matchers {

  implicit private def unsafeLogger[F[_]: Sync]: SelfAwareStructuredLogger[F] =
    Slf4jLogger.getLogger[F]
  implicit val timer: Timer[IO] = IO.timer(concurrent.ExecutionContext.global)

  private val keySchema = getSchema("key")
  private val valueSchema = getSchema("val")

  "CreateTopicSpec" must {
    "register the two avro schemas" in {
      val schemaRegistryIO = SchemaRegistry.test[IO]
      val policy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp

      (for {
        schemaRegistry <- schemaRegistryIO
        registerInternalMetadata = new CreateTopicProgram[IO](
          schemaRegistry,
          policy
        )
        _ = registerInternalMetadata
          .createTopic("subject", keySchema, valueSchema)
          .unsafeRunSync()
        containsSingleKeyAndValue <- schemaRegistry.getAllSubjects.map(
          _.length == 2
        )
      } yield assert(containsSingleKeyAndValue)).unsafeRunSync()
    }

    "retry on Error" in {
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
          override def getAllVersions(subject: String): IO[List[Int]] = ???
          override def getAllSubjects: IO[List[String]] = ???
        }

      Ref[IO]
        .of(TestState(deleteSchemaWasCalled = false, 0))
        .flatMap { ref =>
          val schemaRegistry = getSchemaRegistry(ref)
          val createTopic = new CreateTopicProgram[IO](schemaRegistry, policy)
          for {
            _ <- createTopic
              .createTopic("subject", keySchema, valueSchema)
              .attempt
            result <- ref.get
          } yield assert(result.deleteSchemaWasCalled)

        }
        .unsafeRunSync()
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
          override def getAllVersions(subject: String): IO[List[Int]] = ???
          override def getAllSubjects: IO[List[String]] = ???
        }

      Ref[IO]
        .of(0)
        .flatMap { ref =>
          val schemaRegistry = getSchemaRegistry(ref)
          val createTopic = new CreateTopicProgram[IO](schemaRegistry, policy)
          for {
            _ <- createTopic
              .createTopic("subject", keySchema, valueSchema)
              .attempt
            result <- ref.get
          } yield result shouldBe numberRetries + 1

        }
        .unsafeRunSync()
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
          override def getAllVersions(subject: String): IO[List[Int]] = ???
          override def getAllSubjects: IO[List[String]] = ???
        }

      val schemaRegistryState = Map("subject-key" -> 1)
      Ref[IO]
        .of(TestState(schemaRegistryState))
        .flatMap { ref =>
          val schemaRegistry = getSchemaRegistry(ref)
          val createTopic = new CreateTopicProgram[IO](schemaRegistry, policy)
          for {
            _ <- createTopic
              .createTopic("subject", keySchema, valueSchema)
              .attempt
            result <- ref.get
          } yield result.schemas shouldBe schemaRegistryState

        }
        .unsafeRunSync()
    }

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
