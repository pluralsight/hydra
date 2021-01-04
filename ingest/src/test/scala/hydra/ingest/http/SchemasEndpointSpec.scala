package hydra.ingest.http

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.TestKit
import cats.effect.IO
import com.typesafe.config.ConfigFactory
import hydra.avro.registry.{ConfluentSchemaRegistry, SchemaRegistry}
import hydra.common.config.ConfigSupport
import hydra.common.util.ActorUtils
import hydra.core.marshallers.{GenericServiceResponse, HydraJsonSupport}
import hydra.ingest.http.mock.MockEndpoint
import hydra.kafka.algebras.{KafkaClientAlgebra, MetadataAlgebra}
import hydra.kafka.consumer.KafkaConsumerProxy
import hydra.kafka.consumer.KafkaConsumerProxy.{GetPartitionInfo, ListTopics, ListTopicsResponse, PartitionInfoResponse}
import hydra.kafka.endpoints.{CreateTopicReq, CreateTopicResponseError, TopicMetadataEndpoint}
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import org.apache.avro.Schema
import org.apache.kafka.common.{Node, PartitionInfo}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import spray.json.{JsArray, JsObject, JsValue, RootJsonFormat}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}

import scala.collection.immutable.Map
import scala.concurrent.duration._
import scala.io.Source

/**
  * Created by alexsilva on 5/12/17.
  */
class SchemasEndpointSpec
    extends Matchers
    with AnyWordSpecLike
    with ScalatestRouteTest
    with HydraJsonSupport
    with HydraKafkaJsonSupport
    with ConfigSupport
    with EmbeddedKafka {

  import ConfigSupport._

  implicit val kafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort = 8062, zooKeeperPort = 3161)

  override def createActorSystem(): ActorSystem =
    ActorSystem(actorSystemNameFrom(getClass))

  val consumerPath: String = applicationConfig
    .getStringOpt("actors.kafka.consumer_proxy.path")
    .getOrElse(
      s"/user/service/${ActorUtils.actorName(classOf[KafkaConsumerProxy])}"
    )

  val consumerProxy: ActorSelection = system.actorSelection(consumerPath)

  val schemasRoute = new SchemasEndpoint(consumerProxy).route
  implicit val endpointFormat = jsonFormat3(SchemasEndpointResponse.apply)
  implicit val endpointV2Format = jsonFormat2(SchemasWithKeyEndpointResponse.apply)
  implicit val schemasWithTopicFormat: RootJsonFormat[SchemasWithTopicResponse] = jsonFormat2(SchemasWithTopicResponse.apply)
  implicit val batchSchemasFormat: RootJsonFormat[BatchSchemasResponse] = {
    val make: List[SchemasWithTopicResponse] => BatchSchemasResponse = BatchSchemasResponse.apply
    jsonFormat1(make)
  }

  private val schemaRegistry =
    ConfluentSchemaRegistry.forConfig(applicationConfig)

  val schema =
    new Schema.Parser().parse(Source.fromResource("schema.avsc").mkString)

  val schemaEvolved =
    new Schema.Parser().parse(Source.fromResource("schema2.avsc").mkString)

  val newSchema =
    new Schema.Parser().parse(Source.fromResource("schema-new.avsc").mkString)


  override def beforeAll = {
    super.beforeAll()
    EmbeddedKafka.start()

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

    Post("/schemas", newSchema.toString) ~> schemasRoute ~> check {
      response.status.intValue() shouldBe 201
      val r = responseAs[SchemasEndpointResponse]
      new Schema.Parser().parse(r.schema) shouldBe newSchema
      r.version shouldBe 1
    }
  }

  override def afterAll = {
    super.afterAll()
    EmbeddedKafka.stop()
    TestKit.shutdownActorSystem(
      system,
      verifySystemShutdown = true,
      duration = 10 seconds
    )
  }

  val node = new Node(0, "host", 1)

  def partitionInfo(name: String) =
    new PartitionInfo(name, 0, node, Array(node), Array(node))

  val topics = Map(
    "hydra.test.Tester" -> Seq(partitionInfo("hydra.test.Tester")),
    "hydra.test.NewTester" -> Seq(partitionInfo(name="hydra.test.NewTester"))
  )

  private implicit val createTopicFormat: RootJsonFormat[CreateTopicReq] = jsonFormat4(CreateTopicReq)

  private implicit val errorFormat: RootJsonFormat[CreateTopicResponseError] = jsonFormat1(CreateTopicResponseError)

  val proxy: ActorRef = system.actorOf(
    Props(new Actor {

      override def receive: Receive = {
        case ListTopics =>
          sender ! ListTopicsResponse(topics)
        case GetPartitionInfo(topic) =>
          sender ! PartitionInfoResponse(topic, Seq(partitionInfo(topic)))
        case x =>
          throw new RuntimeException(s"did not expect $x")
      }
    }),
    "kafka_consumer_proxy_test"
  )

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
          .getId(
            "hydra.test.Tester-value",
            new Schema.Parser().parse(rep.schema)
          )
        rep.id shouldBe id
        rep.version shouldBe 2
        new Schema.Parser().parse(rep.schema) shouldBe schemaEvolved
      }
    }

    "evolves a schema" in {
      Get("/schemas/hydra.test.Tester") ~> schemasRoute ~> check {
        val rep = responseAs[SchemasEndpointResponse]
        val id = schemaRegistry.registryClient
          .getId(
            "hydra.test.Tester-value",
            new Schema.Parser().parse(rep.schema)
          )
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
        response.status
          .intValue() should be >= 400 //have to do this bc the mock registry returns an IOException
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
      val url =
        s"/throwRestClientException?statusCode=$statusCode&errorCode=$errorCode&errorMessage=$errorMessage"
      Get(url) ~> Route.seal(mockRoute) ~> check {
        response.status.intValue() shouldBe statusCode
        val res = responseAs[GenericServiceResponse]
        val resMessage = res.message
        resMessage.contains(errorCode.toString) shouldBe true
        resMessage.contains(errorMessage) shouldBe true
      }
    }

    "return 404 if v2 schema doesn't exist" in {
      Get("/v2/schemas/tester/") ~> schemasRoute ~> check {
        response.status
          .intValue() should be >= 400 //have to do this bc the mock registry returns an IOException
      }
    }

    "return schemas" in {
      Get("/v2/schemas") ~> schemasRoute ~> check {
        val resp = responseAs[BatchSchemasResponse]

        resp.schemasResponse.length shouldBe 2
        val schemaResp1 = resp.schemasResponse.head
        val schemaResp2 = resp.schemasResponse.tail.head

        schemaResp1.topic shouldBe "hydra.test.Tester"
        schemaResp1.valueSchemaResponse.get.schema shouldBe schemaEvolved.toString

        schemaResp2.topic shouldBe "hydra.test.NewTester"
        schemaResp2.valueSchemaResponse.get.schema shouldBe newSchema.toString
      }
    }
  }
}
