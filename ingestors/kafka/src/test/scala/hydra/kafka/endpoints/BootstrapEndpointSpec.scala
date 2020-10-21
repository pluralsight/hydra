package hydra.kafka.endpoints

import akka.actor.{Actor, Props}
import akka.http.javadsl.server.MalformedRequestContentRejection
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.RequestEntityExpectedRejection
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.testkit.TestKit
import com.pluralsight.hydra.avro.JsonConverter
import hydra.avro.registry.ConfluentSchemaRegistry
import hydra.common.config.ConfigSupport
import hydra.core.protocol.{Ingest, IngestorCompleted, IngestorError}
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import hydra.kafka.model.TopicMetadata
import hydra.kafka.producer.AvroRecord
import io.confluent.kafka.serializers.KafkaAvroSerializer
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import spray.json._

import scala.concurrent.duration._
import scala.io.Source

class BootstrapEndpointSpec
    extends Matchers
    with AnyWordSpecLike
    with ScalatestRouteTest
    with HydraKafkaJsonSupport
    with ConfigSupport
    with EmbeddedKafka
    with Eventually {

  private implicit val timeout = RouteTestTimeout(10.seconds)

  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(
    kafkaPort = 8012,
    zooKeeperPort = 3111,
    customBrokerProperties = Map("auto.create.topics.enable" -> "false")
  )

  override implicit val patienceConfig =
    PatienceConfig(timeout = scaled(5000 millis), interval = scaled(100 millis))

  class TestKafkaIngestor extends Actor {

    override def receive = {
      case Ingest(hydraRecord, _)
          if hydraRecord
            .asInstanceOf[AvroRecord]
            .payload
            .get("subject") == "exp.dataplatform.failed" =>
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

  val ingestorRegistry =
    system.actorOf(Props(new IngestorRegistry), "ingestor_registry")

  private val bootstrapRoute = new BootstrapEndpoint(system).route

  implicit val f = jsonFormat11(TopicMetadata)

  override def beforeAll: Unit = {
    EmbeddedKafka.start()
    EmbeddedKafka.createCustomTopic("_hydra.metadata.topic")
  }

  override def afterAll = {
    super.afterAll()
    EmbeddedKafka.stop()
    TestKit.shutdownActorSystem(
      system,
      verifySystemShutdown = true,
      duration = 10.seconds
    )
  }

  "The bootstrap endpoint" should {
    "list streams" in {

      val json =
        s"""{
           |	"id":"79a1627e-04a6-11e9-8eb2-f2801f1b9fd1",
           | "createdDate":"${ISODateTimeFormat
             .basicDateTimeNoMillis()
             .print(DateTime.now)}",
           | "subject": "exp.assessment.SkillAssessmentTopicsScored",
           |	"streamType": "Notification",
           | "derived": false,
           |	"dataClassification": "Public",
           |	"contact": "slackity slack dont talk back",
           |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
           |	"notes": "here are some notes topkek",
           |	"schemaId": 2
           |}""".stripMargin.parseJson
          .convertTo[TopicMetadata]

      val topicMetadataJson =
        Source.fromResource("HydraMetadataTopic.avsc").mkString

      val schema = new Schema.Parser().parse(topicMetadataJson)

      val record: Object = new JsonConverter[GenericRecord](schema)
        .convert(json.toJson.compactPrint)
      implicit val deserializer = new KafkaAvroSerializer(
        ConfluentSchemaRegistry.forConfig(applicationConfig).registryClient
      )
      EmbeddedKafka.publishToKafka("_hydra.metadata.topic", record)

      eventually {
        Get("/streams") ~> bootstrapRoute ~> check {
          val r = responseAs[Seq[TopicMetadata]]
          r.length should be >= 1
          r.head.id.toString shouldBe "79a1627e-04a6-11e9-8eb2-f2801f1b9fd1"
        }
      }
    }

    "get a stream by subject" in {

      val json =
        s"""{
           |	"id":"79a1627e-04a6-11e9-8eb2-f2801f1b9fd1",
           | "createdDate":"${ISODateTimeFormat
             .basicDateTimeNoMillis()
             .print(DateTime.now)}",
           | "subject": "exp.assessment.SkillAssessmentTopicsScored1",
           |	"streamType": "Notification",
           | "derived": false,
           |	"dataClassification": "Public",
           |	"contact": "slackity slack dont talk back",
           |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
           |	"notes": "here are some notes topkek",
           |	"schemaId": 2
           |}""".stripMargin.parseJson
          .convertTo[TopicMetadata]

      val topicMetadataJson =
        Source.fromResource("HydraMetadataTopic.avsc").mkString

      val schema = new Schema.Parser().parse(topicMetadataJson)

      val record: Object = new JsonConverter[GenericRecord](schema)
        .convert(json.toJson.compactPrint)
      implicit val deserializer = new KafkaAvroSerializer(
        ConfluentSchemaRegistry.forConfig(applicationConfig).registryClient
      )
      EmbeddedKafka.publishToKafka("_hydra.metadata.topic", record)

      eventually {
        Get("/streams/exp.assessment.SkillAssessmentTopicsScored1") ~> bootstrapRoute ~> check {
          val r = responseAs[Seq[TopicMetadata]]
          r.length should be >= 1
          r(0).id.toString shouldBe "79a1627e-04a6-11e9-8eb2-f2801f1b9fd1"
        }
      }
    }

    "reject empty requests" in {
      Post("/streams") ~> bootstrapRoute ~> check {
        rejection shouldEqual RequestEntityExpectedRejection
      }
    }

    "complete all 3 steps (ingest metadata, register schema, create topic) for valid requests" in {
      val testEntity = HttpEntity(
        ContentTypes.`application/json`,
        """{
          |	"streamType": "Notification",
          | "derived": false,
          |	"dataClassification": "Public",
          |	"dataSourceOwner": "BARTON",
          |	"contact": "slackity slack dont talk back",
          |	"psDataLake": false,
          |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
          |	"notes": "here are some notes topkek",
          |	"schema": {
          |	  "namespace": "exp.assessment",
          |	  "name": "SkillAssessmentTopicsScored",
          |	  "type": "record",
          |	  "version": 1,
          |	  "fields": [
          |	    {
          |	      "name": "testField",
          |	      "type": "string"
          |	    }
          |	  ]
          |	}
          |}""".stripMargin
      )

      Post("/streams", testEntity) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.OK
        val r = responseAs[TopicMetadata]
        r.streamType shouldBe "Notification"
      }
    }

    "return the correct response when the ingestor fails" in {
      val testEntity = HttpEntity(
        ContentTypes.`application/json`,
        """{
          |	"streamType": "Notification",
          | "derived": false,
          |	"dataClassification": "Public",
          |	"dataSourceOwner": "BARTON",
          |	"contact": "slackity slack dont talk back",
          |	"psDataLake": false,
          |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
          |	"notes": "here are some notes topkek",
          |	"schema": {
          |	  "namespace": "exp.dataplatform",
          |	  "name": "failed",
          |	  "type": "record",
          |	  "version": 1,
          |	  "fields": [
          |	    {
          |	      "name": "testField",
          |	      "type": "string"
          |	    }
          |	  ]
          |	}
          |}""".stripMargin
      )

      Post("/streams", testEntity) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }

    "reject requests with invalid topic names" in {
      val testEntity = HttpEntity(
        ContentTypes.`application/json`,
        """{
          |	"streamType": "Notification",
          | "derived": false,
          |	"dataClassification": "Public",
          |	"dataSourceOwner": "BARTON",
          |	"contact": "slackity slack dont talk back",
          |	"psDataLake": false,
          |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
          |	"notes": "here are some notes topkek",
          |	"schema": {
          |	  "namespace": "exp",
          |	  "name": "SkillAssessmentTopicsScored",
          |	  "type": "record",
          |	  "version": 1,
          |	  "fields": [
          |	    {
          |	      "name": "testField",
          |	      "type": "string"
          |	    }
          |	  ]
          |	}
          |}""".stripMargin
      )

      Post("/streams", testEntity) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }

    "reject requests with invalid generic schema" in {
      val testEntity = HttpEntity(
        ContentTypes.`application/json`,
        """{
          |	"streamType": "Notification",
          | "derived": false,
          |	"dataClassification": "Public",
          |	"dataSourceOwner": "BARTON",
          |	"contact": "slackity slack dont talk back",
          |	"psDataLake": false,
          |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
          |	"notes": "here are some notes topkek",
          |	"schema": {
          |	  "name": "SkillAssessmentTopicsScored",
          |	  "type": "record",
          |	  "version": 1,
          |	  "fields": [
          |	    {
          |	      "name": "testField",
          |	      "type": "string"
          |	    }
          |	  ]
          |	}
          |}""".stripMargin
      )

      Post("/streams", testEntity) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }

    "reject invalid metadata payloads" in {
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
          |	      "name": "testField",
          |	      "type": "string"
          |	    }
          |	  ]
          |	}
          |}""".stripMargin
      )

      Post("/streams", testEntity) ~> bootstrapRoute ~> check {
        rejection shouldBe a[MalformedRequestContentRejection]
      }
    }

    "accept previously existing invalid schema" in {
      val testEntity = HttpEntity(
        ContentTypes.`application/json`,
        """{
          |	"streamType": "Notification",
          | "derived": false,
          |	"dataClassification": "Public",
          |	"dataSourceOwner": "BARTON",
          |	"contact": "slackity slack dont talk back",
          |	"psDataLake": false,
          |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
          |	"notes": "here are some notes topkek",
          |	"schema": {
          |   "namespace": "exp.test-existing.v1",
          |	  "name": "SubjectPreexisted",
          |	  "type": "record",
          |	  "version": 1,
          |	  "fields": [
          |	    {
          |	      "name": "testField",
          |	      "type": "string"
          |	    }
          |	  ]
          |	}
          |}""".stripMargin
      )

      val bootstrapRouteWithOverridenStreamManager =
        (new BootstrapEndpoint(system) with BootstrapEndpointTestActors).route
      Post("/streams", testEntity) ~> bootstrapRouteWithOverridenStreamManager ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    "verify that creation date does not change on updates" in {
      val testEntity = HttpEntity(
        ContentTypes.`application/json`,
        """{
          |	"streamType": "Notification",
          | "derived": false,
          |	"dataClassification": "Public",
          |	"dataSourceOwner": "BARTON",
          |	"contact": "slackity slack dont talk back",
          |	"psDataLake": false,
          |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
          |	"notes": "here are some notes topkek",
          |	"schema": {
          |   "namespace": "exp.test-existing.v1",
          |	  "name": "SubjectPreexisted",
          |	  "type": "record",
          |	  "version": 1,
          |	  "fields": [
          |	    {
          |	      "name": "testField",
          |	      "type": "string"
          |	    }
          |	  ]
          |	}
          |}""".stripMargin
      )

      val bootstrapRouteWithOverridenStreamManager =
        (new BootstrapEndpoint(system) with BootstrapEndpointTestActors).route
      Get("/streams/exp.test-existing.v1.SubjectPreexisted") ~> bootstrapRouteWithOverridenStreamManager ~> check {
        val originalTopicData = responseAs[Seq[TopicMetadata]]
        val originalTopicCreationDate = originalTopicData.head.createdDate
        Post("/streams", testEntity) ~> bootstrapRouteWithOverridenStreamManager ~> check {
          status shouldBe StatusCodes.OK
          val response2 = responseAs[TopicMetadata]
          response2.createdDate shouldBe originalTopicCreationDate
        }
      }
    }
  }
}
