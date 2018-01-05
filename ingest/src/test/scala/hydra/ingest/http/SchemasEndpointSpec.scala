package hydra.ingest.http

import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.TestKit
import hydra.avro.registry.ConfluentSchemaRegistry
import hydra.common.config.ConfigSupport
import hydra.core.marshallers.HydraJsonSupport
import org.apache.avro.Schema
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.io.Source

/**
  * Created by alexsilva on 5/12/17.
  */
class SchemasEndpointSpec extends Matchers with WordSpecLike with ScalatestRouteTest
  with HydraJsonSupport with ConfigSupport {

  val schemasRoute = new SchemasEndpoint().route
  implicit val endpointFormat = jsonFormat3(SchemasEndpointResponse.apply)

  private val schemaRegistry = ConfluentSchemaRegistry.forConfig(applicationConfig)

  val schema = new Schema.Parser().parse(Source.fromResource("schema.avsc").mkString)
  schemaRegistry.registryClient.register("hydra.test.Tester-value", schema)

  val schemaEvolved = new Schema.Parser().parse(Source.fromResource("schema2.avsc").mkString)

  override def afterAll = {
    super.afterAll()
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true, duration = 10 seconds)
  }

  "The schemas endpoint" should {

    "returns a list of schemas" in {
      Get("/schemas") ~> schemasRoute ~> check {
        responseAs[Seq[String]] shouldBe Seq("hydra.test.Tester")
      }
    }

    "returns a single schema by name" in {
      Get("/schemas/hydra.test.Tester") ~> schemasRoute ~> check {
        val rep = responseAs[SchemasEndpointResponse]
        rep.id shouldBe 1
        rep.version shouldBe 1
        new Schema.Parser().parse(rep.schema) shouldBe schema
      }
    }

    "evolves a schema" in {
      schemaRegistry.registryClient.register("hydra.test.Tester-value", schemaEvolved)

      Get("/schemas/hydra.test.Tester") ~> schemasRoute ~> check {
        val rep = responseAs[SchemasEndpointResponse]
        rep.id shouldBe 2
        rep.version shouldBe 2
        new Schema.Parser().parse(rep.schema) shouldBe schemaEvolved
      }
    }

    "returns only the schema" in {
      Get("/schemas/hydra.test.Tester?schema") ~> schemasRoute ~> check {
        new Schema.Parser().parse(responseAs[String]) shouldBe schemaEvolved
      }
    }

    "returns 404 if schema doesn't exist" in {
      Get("/schemas/tester") ~> schemasRoute ~> check {
        response.status.intValue() should be >= 400 //have to do this bc the mock registry returns an IOException
      }
    }

    "posts a schema" in {
      val newSchema = new Schema.Parser().parse(Source.fromResource("schema-new.avsc").mkString)
      Post("/schemas", newSchema.toString) ~> schemasRoute ~> check {
        response.status.intValue() shouldBe 201
        val r = responseAs[SchemasEndpointResponse]
        new Schema.Parser().parse(r.schema) shouldBe newSchema
        r.version shouldBe 1
      }
    }

    "gets schema versions" in {
      Get("/schemas/hydra.test.Tester/versions") ~> schemasRoute ~> check {
        val r = responseAs[Seq[SchemasEndpointResponse]]
        r.size shouldBe 2
      }
    }

    "gets a specific schema version" in {
      Get("/schemas/hydra.test.Tester/versions/2") ~> schemasRoute ~> check {
        val r = responseAs[SchemasEndpointResponse]
        new Schema.Parser().parse(r.schema) shouldBe schemaEvolved
      }
    }

    "returns a 400 with a bad schema" in {
      Post("/schemas", "not a schema") ~> schemasRoute ~> check {
        response.status.intValue() shouldBe 400
      }
    }
  }
}
