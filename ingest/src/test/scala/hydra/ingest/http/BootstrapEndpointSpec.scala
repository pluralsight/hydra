package hydra.ingest.http

import akka.actor.{Actor, Props}
import akka.http.javadsl.server.MalformedRequestContentRejection
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.{MethodRejection, RequestEntityExpectedRejection}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.testkit.TestKit
import hydra.common.config.ConfigSupport
import hydra.core.protocol.{Ingest, IngestorCompleted, IngestorError}
import hydra.kafka.producer.AvroRecord
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._


class BootstrapEndpointSpec extends Matchers
  with WordSpecLike
  with ScalatestRouteTest
  with HydraIngestJsonSupport
  with ConfigSupport {

  private implicit val timeout = RouteTestTimeout(10.seconds)

  class TestKafkaIngestor extends Actor {
    override def receive = {
      case Ingest(hydraRecord, _) if hydraRecord.asInstanceOf[AvroRecord].payload.get("streamName") == "exp.dataplatform.failed" =>
        sender ! IngestorError(new Exception("oh noes!"))
      case Ingest(_, _) => sender ! IngestorCompleted
    }
    def props: Props = Props()

  }

  class IngestorRegistry extends Actor {
    context.actorOf(Props(new TestKafkaIngestor), "kafka_ingestor")

    override def receive: Receive = {
      case _ =>
    }
  }

  val ingestorRegistry = system.actorOf(Props(new IngestorRegistry), "ingestor_registry")

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

    "reject empty requests" in {
      Post("/topics") ~> bootstrapRoute ~> check {
        rejection shouldEqual RequestEntityExpectedRejection
      }
    }

    "forward topic metadata to the appropriate handler" in {
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

  "return the correct response when the ingestor fails" in {
    val testEntity = HttpEntity(
      ContentTypes.`application/json`,
      """{
        |	"streamName": "exp.dataplatform.failed",
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

  "reject requests with invalid topic names" in {
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

  "return a BadRequest for invalid metadata payloads" in {
    val testEntity = HttpEntity(
      ContentTypes.`application/json`,
      """{
        |	"streamName": "invalid",
        |	"streamType": "Historical",
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
      rejection shouldBe a[MalformedRequestContentRejection]
    }
  }
}
