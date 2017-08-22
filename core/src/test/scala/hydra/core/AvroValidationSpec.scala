package hydra.core

import com.pluralsight.hydra.avro.RequiredFieldMissingException
import hydra.core.avro.AvroValidation
import hydra.core.avro.schema.SchemaResourceLoader
import hydra.core.ingest.HydraRequest
import hydra.core.protocol.{InvalidRequest, ValidRequest}
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.avro.Schema
import org.scalatest.{FlatSpecLike, Matchers}

import scala.io.Source

class AvroValidationSpec extends Matchers with FlatSpecLike with AvroValidation {

  val client = new MockSchemaRegistryClient()

  client.register("hydra.test.Tester-value",
    new Schema.Parser().parse(Source.fromResource("schema.avsc").mkString))

  lazy val schemaResourceLoader = new SchemaResourceLoader("mock", client)

  def getSubject(request: HydraRequest) = request.metadataValue("schema").get

  it should "return invalid for payloads that do not conform to the schema" in {
    val r = HydraRequest(1,"""{"name":"test"}""").withMetadata("schema" -> "hydra.test.Tester")
    val validation = validate(r)
    validation shouldBe a[InvalidRequest]
    validation.asInstanceOf[InvalidRequest].error.getCause shouldBe a[RequiredFieldMissingException]
  }

  it should "validate good avro payloads" in {
    val r = HydraRequest(1,"""{"name":"test","value":"test"}""").withMetadata("schema" -> "hydra.test.Tester")
    val validation = validate(r)
    validation shouldBe ValidRequest

  }


}
