package hydra.kafka.services

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{TestKit, TestProbe}
import com.pluralsight.hydra.avro.JsonConverter
import com.typesafe.config.ConfigFactory
import hydra.core.marshallers.TopicMetadataRequest
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import hydra.kafka.model.TopicMetadata
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import spray.json._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.Source

class MetadataConsumerActorSpec extends TestKit(ActorSystem("metadata-stream-actor-spec"))
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockFactory
  with EmbeddedKafka
  with HydraKafkaJsonSupport
  with Eventually {

  implicit val ec = system.dispatcher

  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 8092, zooKeeperPort = 3181,
    customBrokerProperties = Map("auto.create.topics.enable" -> "false"))

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(5000 millis),
    interval = scaled(100 millis))

  val config = ConfigFactory.load().getConfig("hydra_kafka.bootstrap-config")

  val topicMetadataJson = Source.fromResource("HydraMetadataTopic.avsc").mkString

  val srClient = new MockSchemaRegistryClient()

  val schema = new Schema.Parser().parse(topicMetadataJson)

  implicit val format = jsonFormat10(TopicMetadata)

  val formatter = ISODateTimeFormat.basicDateTimeNoMillis()

  val testSchema = new Schema.Parser().parse(
    """
      |{
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
    """.stripMargin)

  val testSchemaId = srClient.register("exp.assessment.SkillAssessmentTopicsScored", testSchema)

  val json =
    s"""{
       |	"id":"79a1627e-04a6-11e9-8eb2-f2801f1b9fd1",
       | "createdDate":"${formatter.print(DateTime.now)}",
       | "subject": "exp.assessment.SkillAssessmentTopicsScored",
       |	"streamType": "Notification",
       | "derived": false,
       |	"dataClassification": "Public",
       |	"contact": "slackity slack dont talk back",
       |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
       |	"notes": "here are some notes topkek",
       |	"schemaId": $testSchemaId
       |}"""
      .stripMargin
      .parseJson
      .convertTo[TopicMetadata]

  val kafkaConfig = ConfigFactory.parseString(
    """
      |  poll-interval = 50ms
      |  poll-timeout = 50ms
      |  stop-timeout = 30s
      |  close-timeout = 20s
      |  commit-timeout = 15s
      |  wakeup-timeout = 10s
      |  commit-time-warning=20s
      |  wakeup-debug = true
      |  commit-refresh-interval = infinite
      |  max-wakeups = 2
      |  use-dispatcher = "akka.kafka.default-dispatcher"
      |  wait-close-partition = 500ms
      |  position-timeout = 5s
      |  offset-for-times-timeout = 5s
      |  metadata-request-timeout = 5s
      |
      |  kafka-clients {
      |    enable.auto.commit = false
      |    key.deserializer = org.apache.kafka.common.serialization.StringDeserializer
      |    value.deserializer = io.confluent.kafka.serializers.KafkaAvroDeserializer
      |  }
    """.stripMargin)

  override def beforeAll: Unit = {
    srClient.register("hydra.metadata.topic-value", schema)
    EmbeddedKafka.start()
    EmbeddedKafka.createCustomTopic("hydra.metadata.topic")
    publishRecord()
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }


  def publishRecord() = {
    val record: Object = new JsonConverter[GenericRecord](schema).convert(json.toJson.compactPrint)
    implicit val deserializer = new KafkaAvroSerializer(srClient)
    EmbeddedKafka.publishToKafka("hydra.metadata.topic", record)
  }


  "The MetadataConsumerActor companion" should "create a Kafka stream" in {
    publishRecord()
    val probe = TestProbe()

    val stream = MetadataConsumerActor.createStream(kafkaConfig, srClient,
      "hydra.metadata.topic", probe.ref)(system.dispatcher, ActorMaterializer())

    val s = stream.run()(ActorMaterializer())


    probe.expectMsg(max = 10.seconds, json)
    probe.expectMsg(json)

    //    c.map(_.value().toString.parseJson.asJsObject.fields - "schema")
    //      .runWith(TestSink.probe)(ActorMaterializer())
    //      .request(1)
    //      .expectNext(10.seconds, json.parseJson.asJsObject.fields - "schema")
  }

  it should "bootstrap with the current topics" in {
    //val actor = system.actorOf(Props(classOf[MetadataConsumerActor], kafkaConfig, "hydra.metadata.topic"))
    // actor ! "RAR"
  }
}


