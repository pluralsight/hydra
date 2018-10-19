package hydra.ingest.services

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import hydra.core.http.ImperativeRequestContext
import hydra.core.ingest.HydraRequest
import hydra.core.protocol.InitiateHttpRequest
import hydra.ingest.http.HydraIngestJsonSupport
import hydra.ingest.services.TopicBootstrapActor.InitiateTopicBootstrap
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import hydra.core.ingest.RequestParams.HYDRA_KAFKA_TOPIC_PARAM

class TopicBootstrapActorSpec extends TestKit(ActorSystem("topic-bootstrap-actor-spec"))
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockFactory
  with HydraIngestJsonSupport
  with Eventually {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  val config = ConfigFactory.load()

  val probe = TestProbe()

  val testHandlerGateway: ActorRef = system.actorOf(Props(
    new Actor {

      override def receive = {
        case msg @ InitiateHttpRequest(req, duration, ctx) =>
          ctx.complete(StatusCodes.OK)
          probe.ref forward msg
      }
    }
  ))

  val ctx = new ImperativeRequestContext {
    var completed: ToResponseMarshallable = _
    var error: Throwable = _

    override def complete(obj: ToResponseMarshallable): Unit = completed = obj

    override def failWith(error: Throwable): Unit = this.error = error
  }

  val bootstrapActor = system.actorOf(TopicBootstrapActor.props(config, probe.ref,
    testHandlerGateway))

  "A TopicBootstrapActor" should "initiate a topic bootstrap" in {


    val mdRequest = """{
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
                      |}"""
      .stripMargin

    val hydraReq = HydraRequest("corr_id", mdRequest)

    bootstrapActor ! InitiateTopicBootstrap(hydraReq, ctx)

    probe.expectMsgType[InitiateHttpRequest]

    eventually {
      ctx.completed.value shouldBe StatusCodes.OK
    }

  }

  it should "complete with BadRequest for failures" in {

    val mdRequest = """{
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
                      |}"""
      .stripMargin

    val hydraReq = HydraRequest("corr_id", mdRequest)

    bootstrapActor ! InitiateTopicBootstrap(hydraReq, ctx)

    eventually {
      ctx.completed.value.asInstanceOf[(_, _)]._1 match {
        case t: Any =>
          t shouldBe StatusCodes.BadRequest
      }
    }
  }

  it should "enrich a hydra request with the appropriate metadata" in {

    val mdRequest = """{
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
                      |}"""
      .stripMargin

    val hydraReq = HydraRequest("corr_id", mdRequest)

    bootstrapActor ! InitiateTopicBootstrap(hydraReq, ctx)

    probe.expectMsgPF() {
      case InitiateHttpRequest(req, _, _) =>
        req.metadata should contain(HYDRA_KAFKA_TOPIC_PARAM -> "hydra.metadata-topic")
    }
  }

}
