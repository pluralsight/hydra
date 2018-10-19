package hydra.ingest.services

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
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

class TopicBootstrapActorSpec extends TestKit(ActorSystem("topic-bootstrap-actor-spec"))
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockFactory
  with HydraIngestJsonSupport
  with Eventually {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  "A TopicBootstrapActor" should "initiate a topic bootstrap" in {
    val stubCtx = stub[ImperativeRequestContext]

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

    val bootstrapActor = system.actorOf(TopicBootstrapActor.props(config, probe.ref,
      testHandlerGateway))

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

    bootstrapActor ! InitiateTopicBootstrap(hydraReq, stubCtx)

    probe.expectMsgType[InitiateHttpRequest]

    (stubCtx.complete _)
      .verify(*)
      .once
  }

  it should "complete with BadRequest for failures" in {
    // TODO create test for failure, not just completion
  }

  it should "create a HydraRequest" in {

  }

  it should ""
}
