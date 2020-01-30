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
    val subject = "testSubject"
    for {
      schema <- getSchema[F]("testSchema")
      _ <- schemaRegistry.registerSchema(subject, schema)
      allVersions <- schemaRegistry.getAllVersions(subject)
    } yield {
      it must "add a schema" in {
        allVersions shouldBe List(1)
      }
    }
  }

  private def runTests[F[_]: Monad](schemaRegistry: SchemaRegistry[F]): F[Unit] = {
    for {
      _ <- testAddSubject(schemaRegistry)
    } yield ()
  }

  SchemaRegistry.test[IO].flatMap { schemaRegistry =>
    runTests(schemaRegistry)
  }.unsafeRunSync()

}
