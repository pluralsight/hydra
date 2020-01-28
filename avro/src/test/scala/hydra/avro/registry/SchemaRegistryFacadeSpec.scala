package hydra.avro.registry

import cats.effect.IO
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.avro.SchemaBuilder
import org.scalatest.{FlatSpec, Matchers}
import collection.JavaConverters._

class SchemaRegistryFacadeSpec extends FlatSpec with Matchers {

  it should "register key and value schema" in {
    val schemaRegistryClient = new MockSchemaRegistryClient()
    val facade = SchemaRegistryFacade(schemaRegistryClient)

    val schema = SchemaBuilder.record("key")
      .fields()
        .name("isTrue")
        .`type`()
        .stringType()
        .noDefault()
        .endRecord()

    val subject = "testSubject"

    val registerResource = facade.registerSchemas(subject, schema, schema)

    registerResource.use(_ => IO.unit).unsafeRunSync()
    val allSubjects = schemaRegistryClient.getAllSubjects.asScala.toList
    allSubjects should contain allElementsOf List("-key", "-value").map(subject + _)
  }

}
