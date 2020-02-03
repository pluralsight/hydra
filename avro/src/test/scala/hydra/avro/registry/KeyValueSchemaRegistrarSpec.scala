package hydra.avro.registry

import cats.effect.{IO, Resource, Sync}
import cats.implicits._
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.{FlatSpec, Matchers}
import java.util.concurrent.atomic.AtomicBoolean

class KeyValueSchemaRegistrarSpec extends FlatSpec with Matchers {

  private def getSchema(name: String): Schema =
    SchemaBuilder.record(name)
      .fields()
      .name("isTrue")
      .`type`()
      .stringType()
      .noDefault()
      .endRecord()

  val subject = "testSubject"

  private def getTestResources[F[_]: Sync]: F[(SchemaRegistry[F], Resource[F, Unit])] = {
    for {
      schemaRegistryClient <- SchemaRegistry.test[F]
      facade <- KeyValueSchemaRegistrar.make(schemaRegistryClient)
      registerResource = facade.registerSchemas(subject, getSchema("key"), getSchema("value")) 
    } yield (schemaRegistryClient, registerResource)
  }

  it should "register key and value schema" in {
    getTestResources[IO].flatMap { case (schemaRegistryClient, registerResource) =>
      registerResource.use(_ => IO.unit) *>
      List("-key", "-value").map(subject + _).traverse(schemaRegistryClient.getAllVersions).map { allVersions =>
        allVersions.flatten shouldBe List(1, 1)
      }
    }.unsafeRunSync
  }

  it should "Rollback if an error occurs in a later resource" in {
    getTestResources[IO].flatMap { case (schemaRegistryClient, registerResource) =>
      val getAllVersions = List("-key", "-value").map(subject + _).traverse(schemaRegistryClient.getAllVersions).map(_.flatten)
      val failRegister = registerResource.map { _ =>
        throw new Exception
        ()
      }.use(_ => IO.unit).recover { case _ => () }
      for {
        _ <- failRegister
        allVersions <- getAllVersions
        allSubjects <- schemaRegistryClient.getAllSubjects
      } yield {
        allVersions shouldBe empty
        allSubjects should contain allOf (subject + "-key", subject + "-value")
      }
    }.unsafeRunSync
  }

}
