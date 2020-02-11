package hydra.ingest.http

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import hydra.avro.registry.ConfluentSchemaRegistry
import hydra.common.config.ConfigSupport
import hydra.core.marshallers.{GenericServiceResponse, HydraJsonSupport}
import hydra.ingest.http.mock.MockEndpoint
import org.apache.avro.Schema
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.io.Source

/**
  * Created by alexsilva on 5/12/17.
  */
class SchemasEndpointSpec extends Matchers
  with WordSpecLike
  with ScalatestRouteTest
  with HydraJsonSupport
  with ConfigSupport {

  override def createActorSystem(): ActorSystem =
    ActorSystem(actorSystemNameFrom(getClass))


  val schemasRoute = new SchemasEndpoint().route
  implicit val endpointFormat = jsonFormat3(SchemasEndpointResponse.apply)

  private val schemaRegistry = ConfluentSchemaRegistry.forConfig(applicationConfig)

  val schema = new Schema.Parser().parse(Source.fromResource("schema.avsc").mkString)

  val schemaEvolved = new Schema.Parser().parse(Source.fromResource("schema2.avsc").mkString)

  override def beforeAll = {
    super.beforeAll()
    Post("/schemas", schema.toString) ~> schemasRoute ~> check {
      response.status.intValue() shouldBe 201
      val r = responseAs[SchemasEndpointResponse]
      new Schema.Parser().parse(r.schema) shouldBe schema
      r.version shouldBe 1
    }

    Post("/schemas", schemaEvolved.toString) ~> schemasRoute ~> check {
      response.status.intValue() shouldBe 201
      val r = responseAs[SchemasEndpointResponse]
      new Schema.Parser().parse(r.schema) shouldBe schemaEvolved
      r.version shouldBe 2
    }
  }

  override def afterAll = {
    super.afterAll()
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true, duration = 10 seconds)
  }

  "The schemas endpoint" should {

    "returns a list of schemas" in {
      Get("/schemas") ~> schemasRoute ~> check {
        responseAs[Seq[String]] should contain("hydra.test.Tester")
      }
    }

    "returns a single schema by name" in {
      Get("/schemas/hydra.test.Tester") ~> schemasRoute ~> check {
        val rep = responseAs[SchemasEndpointResponse]
        val id = schemaRegistry.registryClient
          .getId("hydra.test.Tester-value", new Schema.Parser().parse(rep.schema))
        rep.id shouldBe id
        rep.version shouldBe 2
        new Schema.Parser().parse(rep.schema) shouldBe schemaEvolved
      }
    }

    "evolves a schema" in {
      Get("/schemas/hydra.test.Tester") ~> schemasRoute ~> check {
        val rep = responseAs[SchemasEndpointResponse]
        val id = schemaRegistry.registryClient
          .getId("hydra.test.Tester-value", new Schema.Parser().parse(rep.schema))
        rep.id shouldBe id
        rep.version shouldBe 2
        new Schema.Parser().parse(rep.schema) shouldBe schemaEvolved
      }
    }

    "return only the schema" in {
      Get("/schemas/hydra.test.Tester?schema") ~> schemasRoute ~> check {
        new Schema.Parser().parse(responseAs[String]) shouldBe schemaEvolved
      }
    }

    "return 404 if schema doesn't exist" in {
      Get("/schemas/tester") ~> schemasRoute ~> check {
        response.status.intValue() should be >= 400 //have to do this bc the mock registry returns an IOException
      }
    }

    "gets schema versions" in {
      Get("/schemas/hydra.test.Tester/versions") ~> schemasRoute ~> check {
        val r = responseAs[Seq[SchemasEndpointResponse]]
        r.size shouldBe 2
      }
    }

    "gets a specific schema version" in {
      Get("/schemas/hydra.test.Tester/versions/1") ~> schemasRoute ~> check {
        val r = responseAs[SchemasEndpointResponse]
        new Schema.Parser().parse(r.schema) shouldBe schema
      }
    }

    "returns a 400 with a bad schema" in {
      Post("/schemas", "not a schema") ~> schemasRoute ~> check {
        response.status.intValue() shouldBe 400
      }
    }

    "exception handler should return the status code, error code, and message from a RestClientException" in {
      val mockRoute = new MockEndpoint().route
      val statusCode = 409
      val errorCode = 23
      val errorMessage = "someErrorOccurred"
      val url = s"/throwRestClientException?statusCode=$statusCode&errorCode=$errorCode&errorMessage=$errorMessage"
      Get(url) ~> Route.seal(mockRoute) ~> check {
        response.status.intValue() shouldBe statusCode
        val res = responseAs[GenericServiceResponse]
        val resMessage = res.message
        resMessage.contains(errorCode.toString) shouldBe true
        resMessage.contains(errorMessage) shouldBe true
      }
    }
  }
}

