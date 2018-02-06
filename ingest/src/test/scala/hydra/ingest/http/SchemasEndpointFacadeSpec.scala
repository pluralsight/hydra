package hydra.ingest.http

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import akka.util.Timeout
import hydra.core.akka.SchemaRegistryActor.RegisterSchema
import org.apache.avro.{ Schema, SchemaParseException }
import org.scalatest.{ Matchers, WordSpecLike }

class SchemasEndpointFacadeSpec extends TestKit(ActorSystem("SchemasEndpointFacadeSpec"))
  with Matchers
  with WordSpecLike {

  implicit val timeout = Timeout(3.seconds)

  val validSchema = """{
  "namespace": "hydra.test",
  "type": "record",
  "name": "Tester",
  "fields": [
    {
      "name": "name",
      "type": "string"
    }
  ]
}"""

  val invalidSchema = """{
  "namespace": "hydra.test",
  "type": "record",
  "name": "Testy_McTestFace",
  "fields": [
    {
      "name": "name",
      "type": "string"
    }
  ]
}"""

  val schemaSuffix = "-value"

  def fixture(): (TestProbe, SchemasEndpointFacade) = {
    val schemaRegistryActorStub = TestProbe()
    (schemaRegistryActorStub, new SchemasEndpointFacade(schemaRegistryActorStub.ref))
  }

  "The schemas endpoint facade" should {
    "throw SchemaParseException if the schema name contains special characters" in {
      val (schemaRegistryActorStub, subject) = fixture

      intercept[SchemaParseException] {
        subject.registerSchema(invalidSchema)
      }
    }

    "send RegisterSchema message with correct data to SchemaRegistryActor if schema is valid" in {
      val (schemaRegistryActorStub, subject) = fixture()

      val registerSchemaRequest = subject.registerSchema(validSchema)

      schemaRegistryActorStub.expectMsgPF() {
        case RegisterSchema(subject, schema) =>
          subject shouldEqual "hydra.test.Tester-value"
          schema shouldEqual new Schema.Parser().parse(validSchema)
      }
    }
  }
}
