package hydra.ingest.services

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import hydra.avro.resource.SchemaResource
import hydra.core.akka.SchemaRegistryActor.{FetchSchemaRequest, FetchSchemaResponse, RegisterSchemaRequest}
import hydra.core.marshallers.TopicMetadataRequest
import hydra.core.protocol.{Ingest, IngestorCompleted}
import hydra.core.transport.{AckStrategy, HydraRecord}
import hydra.ingest.http.HydraIngestJsonSupport
import hydra.ingest.services.TopicBootstrapActor.InitiateTopicBootstrap
import hydra.kafka.producer.AvroRecord
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
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

  val config = ConfigFactory.load()

  val probe = TestProbe()

  val testSchemaResource = SchemaResource(1, 1, new Schema.Parser().parse(
    """
      |{
      |  "namespace": "hydra.metadata",
      |  "name": "topic",
      |  "type": "record",
      |  "version": 1,
      |  "fields": [
      |    {
      |      "name": "streamName",
      |      "type": "string"
      |    },
      |    {
      |      "name": "streamType",
      |      "type": "string"
      |    },
      |    {
      |      "name": "streamSubType",
      |      "type": "string"
      |    },
      |    {
      |      "name": "dataClassification",
      |      "type": "string"
      |    },
      |    {
      |      "name": "dataSourceOwner",
      |      "type": "string"
      |    },
      |    {
      |      "name": "dataSourceContact",
      |      "type": "string"
      |    },
      |    {
      |      "name": "psDataLake",
      |      "type": "boolean"
      |    },
      |    {
      |      "name": "dataDocPath",
      |      "type": "string"
      |    },
      |    {
      |    	"name": "dataOwnerNotes",
      |    	"type": "string"
      |    },
      |    {
      |    	"name": "streamSchema",
      |    	"type": "string"
      |    }
      |  ]
      |}
    """.stripMargin))

  val testSchemaRegistry: ActorRef = system.actorOf(Props(
    new Actor {
      override def receive: Receive = {
        case msg: FetchSchemaRequest =>
          probe.ref forward msg
          sender ! FetchSchemaResponse(testSchemaResource)

        case msg: RegisterSchemaRequest =>
          probe.ref forward msg
          sender ! FetchSchemaResponse(testSchemaResource)
      }
    }
  ))

  val successfulTestKafkaIngestor: ActorRef = system.actorOf(Props(
    new Actor {
      override def receive = {
        case msg: Ingest[String, GenericRecord] =>
          probe.ref forward msg
      }
    }
  ), "kafka_ingestor")

  "A TopicBootstrapActor" should "process metadata and send an Ingest message to the kafka ingestor" in {

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

    val bootstrapActor = system.actorOf(TopicBootstrapActor.props(config, testSchemaRegistry,
      system.actorSelection("/user/kafka_ingestor")))

    probe.expectMsgType[RegisterSchemaRequest]

    bootstrapActor ! InitiateTopicBootstrap(mdRequest)

    probe.expectMsgType[FetchSchemaRequest]

    // TODO I think the test is failing in the convert portion of the AvroRecordFactory.
    probe.expectMsgPF() {

      case Ingest(msg: HydraRecord[String, GenericRecord], ack) =>
        msg shouldBe an[AvroRecord]
        msg.payload.getSchema.getName shouldBe "topic"
        ack shouldBe AckStrategy.Replicated

      case _ => println(s"WASNT AN INGEST MESSSAGE!!!")
    }
  }

  it should "respond with the appropriate metadata failure message" in {

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
      .parseJson
      .convertTo[TopicMetadataRequest]

    val bootstrapActor = system.actorOf(TopicBootstrapActor.props(config, testSchemaRegistry,
      system.actorSelection("kafka_ingestor")))

    bootstrapActor ! InitiateTopicBootstrap(mdRequest)
  }
}
