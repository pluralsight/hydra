package hydra.avro.registry

import cats.effect.IO
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.util.Try

class SchemaRegistryFacadeSpec extends FlatSpec with Matchers {

  private def getSchema(name: String): Schema =
    SchemaBuilder.record(name)
      .fields()
      .name("isTrue")
      .`type`()
      .stringType()
      .noDefault()
      .endRecord()

  val subject = "testSubject"

  private def getTestResources = {
    val schemaRegistryClient = new MockSchemaRegistryClient()
    val facade = SchemaRegistryFacade(schemaRegistryClient)
    val registerResource =
      facade.registerSchemas(subject, getSchema("key"), getSchema("value"))
    (schemaRegistryClient, registerResource)
  }

  it should "register key and value schema" in {
    val (schemaRegistryClient, registerResource) = getTestResources
    registerResource.use(_ => IO.unit).unsafeRunSync()
    val allSubjects = schemaRegistryClient.getAllSubjects.asScala.toList
    allSubjects should contain allElementsOf List("-key", "-value").map(subject + _)
  }

  it should "Rollback if an error occurs in a later resource" in {
    val (schemaRegistryClient, registerResource) = getTestResources
    Try(registerResource.map { _ =>
      throw new Exception
      ()
    }.use(_ => IO.unit).unsafeRunSync())
    val allKeyVersions = schemaRegistryClient.getAllVersions(subject + "-key")
    val allValueVersions = schemaRegistryClient.getAllVersions(subject + "-value")
    allKeyVersions shouldBe empty
    allValueVersions shouldBe empty
  }

}
