package hydra.avro.registry

import cats.effect.{IO, Resource, Sync}
import cats.implicits._
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.{FlatSpec, Matchers}
import java.util.concurrent.atomic.AtomicBoolean
import cats.Monad
import cats.effect.Bracket
import cats.Applicative
import cats.Traverse

class KeyValueSchemaRegistrarSpec extends FlatSpec with Matchers {

  private def getSchema(name: String): Schema =
    SchemaBuilder.record(name)
      .fields()
      .name("isTrue")
      .`type`()
      .stringType()
      .noDefault()
      .endRecord()

  private val subject = "testSubject"

  private def getTestResources[F[_]: Sync]: F[(SchemaRegistry[F], Resource[F, Unit])] = {
    for {
      schemaRegistryClient <- SchemaRegistry.test[F]
      facade <- KeyValueSchemaRegistrar.make(schemaRegistryClient)
      registerResource = facade.registerSchemas(subject, getSchema("key"), getSchema("value")) 
    } yield (schemaRegistryClient, registerResource)
  }

  private def testRegister[F[_]: Applicative](schemaRegistryClient: SchemaRegistry[F], registerResource: Resource[F, Unit])
                                       (implicit bracket: Bracket[F, Throwable]): F[Unit] = {
    registerResource.use(_ => Applicative[F].pure(())) *>
      List("-key", "-value").map(subject + _).traverse(schemaRegistryClient.getAllVersions).map { allVersions =>
        it should "register key and value schema" in {
          allVersions.flatten shouldBe List(1, 1)
        }
      }
  }

  private def testRollback[F[_]: Applicative](schemaRegistryClient: SchemaRegistry[F], registerResource: Resource[F, Unit])
                                       (implicit bracket: Bracket[F, Throwable]): F[Unit] = {
    val getAllVersions = List("-key", "-value").map(subject + _).traverse(schemaRegistryClient.getAllVersions).map(_.flatten)
      val failRegister = registerResource.map { _ =>
        throw new Exception
        ()
      }.use(_ => Applicative[F].pure(())).recover { case _ => () }
      for {
        _ <- failRegister
        allVersions <- getAllVersions
        allSubjects <- schemaRegistryClient.getAllSubjects
      } yield {
        it should "Rollback if an error occurs in a later resource" in {
          allVersions shouldBe empty
          allSubjects should contain allOf (subject + "-key", subject + "-value")
        }
      }
  }

  private def runTests[F[_]: Sync](implicit bracket: Bracket[F, Throwable]): F[Unit] = {
    for {
      _ <- getTestResources.flatMap((testRegister[F] _).tupled(_))
      _ <- getTestResources.flatMap((testRollback[F] _).tupled(_))
    } yield ()
  }

  runTests[IO].unsafeRunSync()

}
