package hydra.ingest.http

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import akka.util.Timeout
import hydra.core.akka.SchemaFetchActor.RegisterSchema
import org.apache.avro.SchemaParseException
import org.scalatest.WordSpecLike

class SchemasEndpointFacadeSpec extends TestKit(ActorSystem("SchemasEndpointFacadeSpec"))
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
    val schemaRegistryActor = TestProbe()
    (schemaRegistryActor, new SchemasEndpointFacade(schemaRegistryActor.ref, schemaSuffix))
  }

  "The schemas endpoint facade" should {
    "throw SchemaParseException if the schema name contains special characters" in {
      val (schemaRegistryActor, subject) = fixture

      intercept[SchemaParseException] {
        subject.registerSchema(invalidSchema)
      }
    }

    "send RegisterSchema message to SchemaFetchActor if schema is valid" in {
      val (schemaRegistryActor, subject) = fixture()
      subject.registerSchema(validSchema)
      schemaRegistryActor.expectMsgType[RegisterSchema]
    }
  }
}
