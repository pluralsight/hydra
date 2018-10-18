package hydra.ingest.http

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.{MethodRejection, RequestEntityExpectedRejection}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.testkit.TestKit
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._


class BootstrapEndpointSpec extends Matchers
  with WordSpecLike
  with ScalatestRouteTest
  with HydraIngestJsonSupport {

  private implicit val timeout = RouteTestTimeout(10.seconds)


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

      val badRequest = Post("/topics")
      badRequest ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.BadRequest
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
