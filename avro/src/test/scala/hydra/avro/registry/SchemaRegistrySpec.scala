package hydra.avro.registry

import cats.effect.IO
import cats.implicits._
import cats.{Applicative, Monad}
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.{FlatSpec, Matchers}

class SchemaRegistrySpec extends FlatSpec with Matchers {

  private def getSchema[F[_]: Applicative](name: String): F[Schema] =
    Applicative[F].pure {
      SchemaBuilder.record(name)
        .fields()
        .name("isTrue")
        .`type`()
        .stringType()
        .noDefault()
        .endRecord()
    }

  private def testAddSubject[F[_]: Monad](schemaRegistry: SchemaRegistry[F]): F[Unit] = {
    val subject = "testSubjectAdd"
    for {
      schema <- getSchema[F]("testSchemaAdd")
      _ <- schemaRegistry.registerSchema(subject, schema)
      allVersions <- schemaRegistry.getAllVersions(subject)
    } yield {
      it must "add a schema" in {
        allVersions shouldBe List(1)
      }
    }
  }

  private def testDeleteSchemaVersion[F[_]: Monad](schemaRegistry: SchemaRegistry[F]): F[Unit] = {
    val subject = "testSubjectDelete"
    for {
      schema <- getSchema[F]("testSchemaDelete")
      _ <- schemaRegistry.registerSchema(subject, schema)
      version <- schemaRegistry.getVersion(subject, schema)
      delete <- schemaRegistry.deleteSchemaOfVersion(subject, version)
      allVersions <- schemaRegistry.getAllVersions(subject)
    } yield {
      it must "delete a schema version" in {
        allVersions shouldBe List.empty
      }
    }
  }

  private def runTests[F[_]: Monad](schemaRegistry: F[SchemaRegistry[F]]): F[Unit] = {
    for {
      _ <- schemaRegistry.flatMap(testAddSubject[F])
      _ <- schemaRegistry.flatMap(testDeleteSchemaVersion[F])
    } yield ()
  }

  runTests(SchemaRegistry.test[IO]).unsafeRunSync()

}
