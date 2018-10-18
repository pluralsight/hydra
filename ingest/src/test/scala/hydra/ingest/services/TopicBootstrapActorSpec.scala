package hydra.ingest.services

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import hydra.core.http.ImperativeRequestContext
import hydra.core.marshallers.TopicMetadataRequest
import hydra.ingest.http.HydraIngestJsonSupport
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import spray.json._

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

    val bootstrapActor = system.actorOf(TopicBootstrapActor.props(config, probe.ref, probe.ref))

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
      .parseJson
      .convertTo[TopicMetadataRequest]

    bootstrapActor ! InitiateTopicBootstrap(mdRequest, stubCtx)

    Thread.sleep(100)

    (stubCtx.complete _)
      .verify(*) // TODO figure out if we can mock ToResponseMarshallable
      .once
  }

  it should "complete with BadRequest for failures" in {
    // TODO create test for failure, not just completion
    fail()
  }

  it should "create a HydraRequest" in {

  }

  it should ""
}
