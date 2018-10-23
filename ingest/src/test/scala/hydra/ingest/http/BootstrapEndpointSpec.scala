package hydra.ingest.http

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.{MethodRejection, RequestEntityExpectedRejection}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.testkit.{TestKit, TestProbe}
import hydra.common.config.ConfigSupport
import hydra.core.protocol.{Ingest, IngestorCompleted}
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._


class BootstrapEndpointSpec extends Matchers
  with WordSpecLike
  with ScalatestRouteTest
  with HydraIngestJsonSupport
  with ConfigSupport {


  //because actors within this endpoint are private, we need to create test instances of them like below.
  private implicit val timeout = RouteTestTimeout(10.seconds)

  class TestKafkaIngestor extends Actor {
    override def receive = {
      case Ingest(_, _) => sender ! IngestorCompleted

    }
    def props: Props = Props()

  }

  val ingestorRegistryProbe = TestProbe("ingestor_registry")

  val ingestorProbe = ingestorRegistryProbe.childActorOf(Props(new TestKafkaIngestor), "kafka_ingestor")

  private val bootstrapRoute = new BootstrapEndpoint().route

  override def afterAll = {
    super.afterAll()
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true, duration = 10.seconds)
  }

  "The bootstrap endpoint" should {

    "rejects a GET request" in {
      Get("/topics") ~> bootstrapRoute ~> check {
        rejections should contain allElementsOf Seq(MethodRejection(HttpMethods.POST))
      }
    }

    "rejects empty requests" in {
      Post("/topics") ~> bootstrapRoute ~> check {
        rejection shouldEqual RequestEntityExpectedRejection
      }
    }

    "forwards topic metadata to the appropriate handler" in {
      val testEntity = HttpEntity(
        ContentTypes.`application/json`,
        """{
          |	"streamName": "exp.dataplatform.testsubject",
          |	"streamType": "Historical",
          |	"streamSubType": "Source Of Truth",
          |	"dataClassification": "Public",
          |	"dataSourceOwner": "BARTON",
          |	"dataSourceContact": "slackity slack dont talk back",
          |	"psDataLake": false,
          |	"dataDocPath": "akka://some/path/here.jpggifyo",
          |	"dataOwnerNotes": "here are some notes topkek",
          |	"streamSchema": {
          |	  "namespace": "exp.assessment",
          |	  "name": "SkillAssessmentTopicsScored",
          |	  "type": "record",
          |	  "version": 1,
          |	  "fields": [
          |	    {
          |	      "name": "test-field",
          |	      "type": "string"
          |	    }
          |	  ]
          |	}
          |}""".stripMargin)

      Post("/topics", testEntity) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.OK
      }
    }
  }

  "rejects requests with invalid topic names" in {
    val testEntity = HttpEntity(
      ContentTypes.`application/json`,
      """{
        |	"streamName": "invalid",
        |	"streamType": "Historical",
        |	"streamSubType": "Source Of Truth",
        |	"dataClassification": "Public",
        |	"dataSourceOwner": "BARTON",
        |	"dataSourceContact": "slackity slack dont talk back",
        |	"psDataLake": false,
        |	"dataDocPath": "akka://some/path/here.jpggifyo",
        |	"dataOwnerNotes": "here are some notes topkek",
        |	"streamSchema": {
        |	  "namespace": "exp.assessment",
        |	  "name": "SkillAssessmentTopicsScored",
        |	  "type": "record",
        |	  "version": 1,
        |	  "fields": [
        |	    {
        |	      "name": "test-field",
        |	      "type": "string"
        |	    }
        |	  ]
        |	}
        |}""".stripMargin)

    Post("/topics", testEntity) ~> bootstrapRoute ~> check {
      status shouldBe StatusCodes.BadRequest
    }
  }

}
