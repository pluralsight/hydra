package hydra.kafka.services

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import com.pluralsight.hydra.avro.JsonConverter
import com.typesafe.config.{ConfigFactory, ConfigValue, ConfigValueFactory}
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import hydra.kafka.model.TopicMetadata
import hydra.kafka.services.StreamsManagerActor.{GetMetadata, GetMetadataResponse, GetStreamActor, GetStreamActorResponse, InitializedStream, MetadataProcessed, TopicMetadataMessage}
import hydra.kafka.util.KafkaUtils
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.BeforeAndAfterAll
import spray.json._

import scala.concurrent.duration._
import scala.io.Source
import akka.actor.ActorRef
import org.apache.kafka.clients.producer.ProducerRecord

class StreamsManagerActorSpec
    extends TestKit(ActorSystem("metadata-stream-actor-spec"))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockFactory
    with ScalaFutures
    with EmbeddedKafka
    with HydraKafkaJsonSupport
    with Eventually {

  implicit val ec = system.dispatcher

  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(
    kafkaPort = 8092,
    zooKeeperPort = 3181,
    customBrokerProperties = Map("auto.create.topics.enable" -> "false")
  )

  val bootstrapConfig =
    ConfigFactory.load().getConfig("hydra_kafka.bootstrap-config")

  val bootstrapServers = KafkaUtils.BootstrapServers

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(5000 millis),
    interval = scaled(1000 millis)
  )

  val config = ConfigFactory.load().getConfig("hydra_kafka.bootstrap-config")

  val topicMetadataJson =
    Source.fromResource("HydraMetadataTopic.avsc").mkString

  val srClient = new MockSchemaRegistryClient()

  val schema = new Schema.Parser().parse(topicMetadataJson)

  implicit val format = jsonFormat12(TopicMetadata)

  val formatter = ISODateTimeFormat.basicDateTimeNoMillis()

  val testSchema =
    new Schema.Parser().parse("""
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

  val testSchemaId =
    srClient.register("exp.assessment.SkillAssessmentTopicsScored", testSchema)

  val json = {
    val tm = s"""{
       |	"id":"79a1627e-04a6-11e9-8eb2-f2801f1b9fd1",
       | "createdDate":"${formatter.print(DateTime.now)}",
       | "subject": "exp.assessment.SkillAssessmentTopicsScored",
       |	"streamType": "Notification",
       | "derived": false,
       |	"dataClassification": "Public",
       |	"contact": "slackity slack dont talk back",
       |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
       |	"notes": "here are some notes topkek",
       |	"schemaId": $testSchemaId,
       |  "notificationUrl": "notification.url"
       |}""".stripMargin.parseJson
      .convertTo[TopicMetadata]
      TopicMetadataMessage("exp.assessment.SkillAssessmentTopicsScored", Some(tm))
    }

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
      |  eos-draining-check-interval = 30ms
      |  partition-handler-warning = 5s
      |
      |  connection-checker {
      |
      |    enable = false
      |
      |    max-retries = 3
      |
      |    check-interval = 15s
      |
      |    backoff-factor = 2.0
      |  }
      |
      |  kafka-clients {
      |    enable.auto.commit = false
      |    key.deserializer = org.apache.kafka.common.serialization.StringDeserializer
      |    value.deserializer = io.confluent.kafka.serializers.KafkaAvroDeserializer
      |  }
    """.stripMargin
  )

  override def beforeAll: Unit = {
    srClient.register("hydra.metadata.topic-value", schema)
    EmbeddedKafka.start()
    EmbeddedKafka.createCustomTopic("hydra.metadata.topic")
    publishRecord(json)
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  def publishRecord(topicMetadataMessage: TopicMetadataMessage) = {
    val record: Object = topicMetadataMessage.topicMetadata.map { tm =>
      new JsonConverter[GenericRecord](schema).convert(tm.toJson.compactPrint)
    }.orNull
    implicit val deserializer = new KafkaAvroSerializer(srClient)
    val pr = new ProducerRecord("hydra.metadata.topic", topicMetadataMessage.subject, record)
    EmbeddedKafka.publishToKafka(pr)
  }

  "The MetadataConsumerActor companion" should "create a Kafka stream" in {
    publishRecord(json)
    val probe = TestProbe()

    val stream = StreamsManagerActor.createMetadataStream(
      kafkaConfig,
      "localhost:8092",
      srClient,
      "hydra.metadata.topic",
      probe.ref
    )(system.dispatcher, system)

    val s = stream.run()

    probe.expectMsg(InitializedStream)
    probe.reply(MetadataProcessed)
    probe.expectMsg(max = 10.seconds, json)
    probe.reply(MetadataProcessed)
    probe.expectMsg(json)

  }

  it should "create a stream even if hydra.key is missing" in {
    val schemaWithKey =
      new Schema.Parser().parse("""
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

    val schemaWKeyId = srClient.register(
      "exp.assessment.SkillAssessmentTopicsScored",
      schemaWithKey
    )

    val metadata =
      s"""{
         |	"id":"79a1627e-04a6-11e9-8eb2-f2801f1b9fd1",
         | "createdDate":"${formatter.print(DateTime.now)}",
         | "subject": "exp.assessment.SkillAssessmentTopicsScored",
         |	"streamType": "History",
         | "derived": false,
         |	"dataClassification": "Public",
         |	"contact": "slackity slack dont talk back",
         |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
         |	"notes": "here are some notes topkek",
         |	"schemaId": $schemaWKeyId,
         |  "notificationUrl": "notification.url"
         |}""".stripMargin.parseJson
        .convertTo[TopicMetadata]

    val streamsManagerActor = system.actorOf(
      StreamsManagerActor.props(bootstrapConfig, bootstrapServers, srClient),
      name = "stream_manager2"
    )
    val topicName = "exp.assessment.SkillAssessmentTopicsScored"

    streamsManagerActor ! metadata

    import akka.pattern.ask

    implicit val timeout = Timeout(3.seconds)

    whenReady(streamsManagerActor ? GetStreamActor(topicName)) { res =>
      res shouldBe GetStreamActorResponse(None)
    }

  }

  it should "create a topic stream if streamType isn't History from the metadata payload" in {
    val schemaWithKey =
      new Schema.Parser().parse("""
        |{
        |	  "namespace": "exp.assessment",
        |	  "name": "SkillAssessmentTopicsScored",
        |	  "type": "record",
        |   "hydra.key": "testField",
        |	  "version": 1,
        |	  "fields": [
        |	    {
        |	      "name": "testField",
        |	      "type": "string"
        |	    }
        |	  ]
        |	}
      """.stripMargin)

    val schemaWKeyId = srClient.register(
      "exp.assessment.SkillAssessmentTopicsScored",
      schemaWithKey
    )

    val metadata =
      s"""{
         |	"id":"79a1627e-04a6-11e9-8eb2-f2801f1b9fd1",
         | "createdDate":"${formatter.print(DateTime.now)}",
         | "subject": "exp.assessment.SkillAssessmentTopicsScored",
         |	"streamType": "CurrentState",
         | "derived": false,
         |	"dataClassification": "Public",
         |	"contact": "slackity slack dont talk back",
         |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
         |	"notes": "here are some notes topkek",
         |	"schemaId": $schemaWKeyId,
         |  "notificationUrl": "notification.url"
         |}""".stripMargin.parseJson
        .convertTo[TopicMetadata]

    val streamsManagerActor = system.actorOf(
      StreamsManagerActor.props(bootstrapConfig, bootstrapServers, srClient),
      name = "stream_manager3"
    )
    val topicName = "exp.assessment.SkillAssessmentTopicsScored"

    streamsManagerActor ! metadata

    import akka.pattern.ask

    implicit val timeout = Timeout(3.seconds)

    whenReady(streamsManagerActor ? GetStreamActor(topicName)) { res =>
      res shouldBe GetStreamActorResponse(None)
    }

  }

  it should "respond with MetadataProcessed after TopicMetadata is received" in {
    val streamsManagerActor: ActorRef = system.actorOf(
      StreamsManagerActor.props(bootstrapConfig, bootstrapServers, srClient),
      name = "stream_manager4"
    )
    val topicMetadata = s"""{
         |	"id":"79a1627e-04a6-11e9-8eb2-f2801f1b9fd1",
         | "createdDate":"${formatter.print(DateTime.now)}",
         | "subject": "exp.assessment.SkillAssessmentTopicsScored",
         |	"streamType": "History",
         | "derived": false,
         |	"dataClassification": "Public",
         |	"contact": "slackity slack dont talk back",
         |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
         |	"notes": "here are some notes topkek",
         |	"schemaId": 1,
         |  "notificationUrl": "notification.url"
         |}""".stripMargin.parseJson
      .convertTo[TopicMetadata]
    val probe = TestProbe()
    streamsManagerActor.tell(topicMetadata, probe.ref)
    probe.expectMsg(MetadataProcessed)
  }

  it should "respond with MetadataProcessed after TopicMetadataMessage is received" in {
    val streamsManagerActor: ActorRef = system.actorOf(
      StreamsManagerActor.props(bootstrapConfig, bootstrapServers, srClient),
      name = "stream_manager5"
    )
    val topicMetadata = s"""{
         |	"id":"79a1627e-04a6-11e9-8eb2-f2801f1b9fd1",
         | "createdDate":"${formatter.print(DateTime.now)}",
         | "subject": "exp.assessment.SkillAssessmentTopicsScored",
         |	"streamType": "History",
         | "derived": false,
         |	"dataClassification": "Public",
         |	"contact": "slackity slack dont talk back",
         |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
         |	"notes": "here are some notes topkek",
         |	"schemaId": 1,
         |  "notificationUrl": "notification.url"
         |}""".stripMargin.parseJson
      .convertTo[TopicMetadata]
    val probe = TestProbe()
    streamsManagerActor.tell(TopicMetadataMessage("exp.assessment.SkillAssessmentTopicsScored", Some(topicMetadata)), probe.ref)
    probe.expectMsg(MetadataProcessed)
  }

  it should "respond with MetadataProcessed after TopicMetadataMessage with None for value is received" in {
    val streamsManagerActor: ActorRef = system.actorOf(
      StreamsManagerActor.props(bootstrapConfig, bootstrapServers, srClient),
      name = "stream_manager6"
    )
    val probe = TestProbe()
    streamsManagerActor.tell(TopicMetadataMessage("exp.assessment.SkillAssessmentTopicsScored", None), probe.ref)
    probe.expectMsg(MetadataProcessed)
  }

  it should "remove metadata from metadataMap after a null value is received" in {
    val streamsManagerActor: ActorRef = system.actorOf(
      StreamsManagerActor.props(bootstrapConfig, bootstrapServers, srClient),
      name = "stream_manager7"
    )
    val topicMetadata = s"""{
                           |	"id":"79a1627e-04a6-11e9-8eb2-f2801f1b9fd1",
                           | "createdDate":"${formatter.print(DateTime.now)}",
                           | "subject": "exp.assessment.SkillAssessmentTopicsScored",
                           |	"streamType": "History",
                           | "derived": false,
                           |	"dataClassification": "Public",
                           |	"contact": "slackity slack dont talk back",
                           |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
                           |	"notes": "here are some notes topkek",
                           |	"schemaId": 1,
                           |  "notificationUrl": "notification.url"
                           |}""".stripMargin.parseJson
      .convertTo[TopicMetadata]
    val probe = TestProbe()
    val expectedMap: Map[String, TopicMetadata] = Map[String, TopicMetadata]() + (topicMetadata.subject -> topicMetadata)
    streamsManagerActor.tell(TopicMetadataMessage("exp.assessment.SkillAssessmentTopicsScored", Some(topicMetadata)), probe.ref)
    probe.expectMsg(MetadataProcessed)
    streamsManagerActor.tell(GetMetadata, probe.ref)
    probe.expectMsg(GetMetadataResponse(expectedMap))
    streamsManagerActor.tell(TopicMetadataMessage("exp.assessment.SkillAssessmentTopicsScored", None), probe.ref)
    probe.expectMsg(MetadataProcessed)
    val newExpectedMap = expectedMap - (topicMetadata.subject)
    streamsManagerActor.tell(GetMetadata, probe.ref)
    probe.expectMsg(GetMetadataResponse(newExpectedMap))
  }

}
