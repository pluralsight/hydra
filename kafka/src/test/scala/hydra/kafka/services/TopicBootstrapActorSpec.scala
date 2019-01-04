package hydra.kafka.services

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorSystem, Props}
import akka.pattern.pipe
import akka.testkit.{TestKit, TestProbe}
import com.pluralsight.hydra.avro.JsonConverter
import com.typesafe.config.ConfigFactory
import hydra.avro.resource.SchemaResource
import hydra.core.akka.SchemaRegistryActor._
import hydra.core.marshallers.TopicMetadataRequest
import hydra.core.protocol.{Ingest, IngestorCompleted}
import hydra.core.transport.AckStrategy
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import hydra.kafka.model.TopicMetadata
import hydra.kafka.producer.AvroRecord
import hydra.kafka.services.TopicBootstrapActor.{BootstrapFailure, BootstrapSuccess, GetStreams, InitiateTopicBootstrap}
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.joda.time.DateTime
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import spray.json._

import scala.concurrent.Future
import scala.io.Source

class TopicBootstrapActorSpec extends TestKit(ActorSystem("topic-bootstrap-actor-spec"))
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockFactory
  with EmbeddedKafka
  with HydraKafkaJsonSupport
  with Eventually {

  import hydra.kafka.services.ErrorMessages._

  implicit val ec = system.dispatcher
  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 8092, zooKeeperPort = 3181,
    customBrokerProperties = Map("auto.create.topics.enable" -> "false"))

  override def beforeAll: Unit = {
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  val config = ConfigFactory.load().getConfig("hydra_kafka.bootstrap-config")

  val topicMetadataJson = Source.fromResource("HydraMetadataTopic.avsc").mkString

  val testSchemaResource = SchemaResource(1, 1, new Schema.Parser().parse(topicMetadataJson))

  def fixture(key: String, kafkaShouldFail: Boolean = false,
              schemaRegistryShouldFail: Boolean = false) = {
    val probe = TestProbe()

    val schemaRegistryActor = system.actorOf(
      Props(
        new Actor {
          override def receive: Receive = {
            case msg: FetchSchemaRequest =>
              sender ! FetchSchemaResponse(testSchemaResource)
              probe.ref forward msg

            case msg: RegisterSchemaRequest =>
              if (schemaRegistryShouldFail) {
                sender ! Failure(new Exception("Schema registry actor failed expectedly!"))
                probe.ref forward msg
              } else {
                sender ! RegisterSchemaResponse(testSchemaResource)
                probe.ref forward msg
              }
          }
        }
      ), s"test-schema-registry-$key")

    val kafkaIngestor = system.actorOf(Props(
      new Actor {
        override def receive = {
          case msg: Ingest[_, _] =>
            probe.ref forward msg
            if (kafkaShouldFail) {
              Future.failed(new Exception("Kafka ingestor failed expectedly!")) pipeTo sender
            } else {
              sender ! IngestorCompleted
            }
        }
      }
    ), s"kafka_ingestor_$key")

    (probe, schemaRegistryActor, kafkaIngestor)
  }


  "A TopicBootstrapActor" should "process metadata and send an Ingest message to the kafka ingestor" in {

    val mdRequest = """{
                      |	"subject": "exp.dataplatform.testsubject1",
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
                      |}"""
      .stripMargin
      .parseJson
      .convertTo[TopicMetadataRequest]

    val (probe, schemaRegistryActor, kafkaIngestor) = fixture("test1")

    val bootstrapActor = system.actorOf(TopicBootstrapActor.props(schemaRegistryActor,
      system.actorSelection("/user/kafka_ingestor_test1")))

    probe.expectMsgType[RegisterSchemaRequest]

    bootstrapActor ! InitiateTopicBootstrap(mdRequest)

    probe.receiveWhile(messages = 2) {
      case RegisterSchemaRequest(schemaJson) => schemaJson should
        include("SkillAssessmentTopicsScored")
      case FetchSchemaRequest(schemaName) => schemaName shouldEqual "_hydra.metadata.topic"
    }

    probe.expectMsgPF() {
      case Ingest(msg: AvroRecord, ack) =>
        msg.payload.getSchema.getName shouldBe "topic"
        ack shouldBe AckStrategy.Replicated
    }
  }

  it should "respond with the error that caused the failed actor state" in {
    val mdRequest = """{
                      |	"subject": "exp.dataplatform.testsubject2",
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
                      |}"""
      .stripMargin
      .parseJson
      .convertTo[TopicMetadataRequest]

    val (probe, schemaRegistryActor, _) = fixture("test2",
      schemaRegistryShouldFail = true)

    val bootstrapActor = system.actorOf(TopicBootstrapActor.props(schemaRegistryActor,
      system.actorSelection("kafka_ingestor_test2")))

    probe.expectMsgType[RegisterSchemaRequest]

    val senderProbe = TestProbe()

    bootstrapActor.tell(InitiateTopicBootstrap(mdRequest), senderProbe.ref)

    senderProbe.expectMsgPF() {
      case Failure(ex) => ex.getMessage shouldEqual
        "TopicBootstrapActor is in a failed state due to cause: Schema registry actor failed expectedly!"
    }
  }

  it should "respond with the appropriate metadata failure message" in {

    val mdRequest = """{
                      |	"subject": "exp....",
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
                      |}"""
      .stripMargin
      .parseJson
      .convertTo[TopicMetadataRequest]

    val (probe, schemaRegistryActor, kafkaIngestor) = fixture("test3")

    val bootstrapActor = system.actorOf(TopicBootstrapActor.props(schemaRegistryActor,
      system.actorSelection("kafka_ingestor_test3")))

    probe.expectMsgType[RegisterSchemaRequest]

    val senderProbe = TestProbe()

    bootstrapActor.tell(InitiateTopicBootstrap(mdRequest), senderProbe.ref)

    senderProbe.expectMsgPF() {
      case BootstrapFailure(reasons) =>
        reasons should contain(BadTopicFormatError)
    }
  }

  it should "respond with the appropriate failure when the KafkaIngestor returns an exception" in {
    val mdRequest = """{
                      |	"subject": "exp.dataplatform.testsubject4",
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
                      |}"""
      .stripMargin
      .parseJson
      .convertTo[TopicMetadataRequest]

    val (probe, schemaRegistryActor, kafkaIngestor) = fixture("test4", kafkaShouldFail = true)

    val bootstrapActor = system.actorOf(TopicBootstrapActor.props(schemaRegistryActor,
      system.actorSelection("/user/kafka_ingestor_test4")))

    probe.expectMsgType[RegisterSchemaRequest]

    val senderProbe = TestProbe()

    bootstrapActor.tell(InitiateTopicBootstrap(mdRequest), senderProbe.ref)

    senderProbe.expectMsgPF() {
      case BootstrapFailure(reasons) =>
        reasons should contain("Kafka ingestor failed expectedly!")
    }
  }

  it should "create the kafka topic" in {
    val subject = "exp.dataplatform.testsubject5"

    val mdRequest = s"""{
                       |	"subject": "$subject",
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
                       |}"""
      .stripMargin
      .parseJson
      .convertTo[TopicMetadataRequest]

    val (probe, schemaRegistryActor, kafkaIngestor) = fixture("test5")

    val bootstrapActor = system.actorOf(TopicBootstrapActor.props(schemaRegistryActor,
      system.actorSelection("/user/kafka_ingestor_test5")))

    probe.expectMsgType[RegisterSchemaRequest]

    val senderProbe = TestProbe()

    bootstrapActor.tell(InitiateTopicBootstrap(mdRequest), senderProbe.ref)

    probe.receiveWhile(messages = 2) {
      case RegisterSchemaRequest(schemaJson) => schemaJson should
        include("SkillAssessmentTopicsScored")
      case FetchSchemaRequest(schemaName) => schemaName shouldEqual "_hydra.metadata.topic"
    }

    probe.expectMsgPF() {
      case Ingest(msg: AvroRecord, ack) =>
        msg.payload.getSchema.getName shouldBe "topic"
        ack shouldBe AckStrategy.Replicated
    }

    senderProbe.expectMsgPF() {
      case BootstrapSuccess(tm) =>
        tm.schemaId should be > 0
        tm.derived shouldBe false
        tm.subject shouldBe subject
    }

    val expectedMessage = "I'm expected!"

    publishStringMessageToKafka(subject, expectedMessage)

    consumeFirstStringMessageFrom(subject) shouldEqual expectedMessage
  }

  it should "return a BootstrapFailure for failures while creating the kafka topic" in {
    val subject = "exp.dataplatform.testsubject6"

    val mdRequest = s"""{
                       |	"subject": "$subject",
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
                       |}"""
      .stripMargin
      .parseJson
      .convertTo[TopicMetadataRequest]

    val testConfig = ConfigFactory.parseString(
      """
        |{
        |  partitions = 3
        |  replication-factor = 3
        |  timeout = 300
        |  failure-retry-millis = 3000
        |}
      """.stripMargin)

    val (probe, schemaRegistryActor, kafkaIngestor) = fixture("test6")

    val bootstrapActor = system.actorOf(TopicBootstrapActor.props(schemaRegistryActor,
      system.actorSelection("/user/kafka_ingestor_test6"), Some(testConfig)))

    probe.expectMsgType[RegisterSchemaRequest]

    val senderProbe = TestProbe()

    bootstrapActor.tell(InitiateTopicBootstrap(mdRequest), senderProbe.ref)

    probe.receiveWhile(messages = 2) {
      case RegisterSchemaRequest(schemaJson) => schemaJson should
        include("SkillAssessmentTopicsScored")
      case FetchSchemaRequest(schemaName) => schemaName shouldEqual "_hydra.metadata.topic"
    }

    probe.expectMsgPF() {
      case Ingest(msg: AvroRecord, ack) =>
        msg.payload.getSchema.getName shouldBe "topic"
        ack shouldBe AckStrategy.Replicated
    }

    senderProbe.expectMsgPF() {
      case BootstrapFailure(reasons) =>
        reasons.nonEmpty shouldBe true
        reasons.head should include("Replication factor: 3 larger than available brokers: 1.")
    }
  }

  it should "not fail due to a topic already existing" in {
    val subject = "exp.dataplatform.testsubject7"

    val mdRequest = s"""{
                       |	"subject": "$subject",
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
                       |}"""
      .stripMargin
      .parseJson
      .convertTo[TopicMetadataRequest]

    val (probe, schemaRegistryActor, kafkaIngestor) = fixture("test7")

    val bootstrapActor = system.actorOf(TopicBootstrapActor.props(schemaRegistryActor,
      system.actorSelection("/user/kafka_ingestor_test7")))

    probe.expectMsgType[RegisterSchemaRequest]

    val senderProbe = TestProbe()

    bootstrapActor.tell(InitiateTopicBootstrap(mdRequest), senderProbe.ref)

    probe.receiveWhile(messages = 2) {
      case RegisterSchemaRequest(schemaJson) => schemaJson should
        include("SkillAssessmentTopicsScored")
      case FetchSchemaRequest(schemaName) => schemaName shouldEqual "_hydra.metadata.topic"
    }

    probe.expectMsgPF() {
      case Ingest(msg: AvroRecord, ack) =>
        msg.payload.getSchema.getName shouldBe "topic"
        ack shouldBe AckStrategy.Replicated
    }

    senderProbe.expectMsgPF() {
      case BootstrapSuccess(tm) =>
        tm.schemaId should be > 0
        tm.derived shouldBe false
        tm.subject shouldBe subject
    }

    bootstrapActor.tell(InitiateTopicBootstrap(mdRequest), senderProbe.ref)

    senderProbe.expectMsgPF() {
      case BootstrapSuccess(tm) =>
        tm.schemaId should be > 0
        tm.derived shouldBe false
        tm.subject shouldBe subject
    }
  }

  it should "retrieve all streams" in {
    val (probe, schemaRegistryActor, _) = fixture("test-stream",
      schemaRegistryShouldFail = false)

    val bootstrapActor = system.actorOf(TopicBootstrapActor.props(schemaRegistryActor,
      system.actorSelection("kafka_ingestor_test-stream")))

    probe.expectMsgType[RegisterSchemaRequest]

    val senderProbe = TestProbe()

    bootstrapActor.tell(GetStreams(None), senderProbe.ref)

    senderProbe.expectMsg("OK")
  }

  it should "retry after a configurable interval when the schema registration fails" in {
    val mdRequest = """{
                      |	"subject": "exp.dataplatform.testsubject8",
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
                      |}"""
      .stripMargin
      .parseJson
      .convertTo[TopicMetadataRequest]

    val (probe, schemaRegistryActor, _) = fixture("test8",
      schemaRegistryShouldFail = true)

    val bootstrapActor = system.actorOf(TopicBootstrapActor.props(schemaRegistryActor,
      system.actorSelection("kafka_ingestor_test8")))

    probe.expectMsgType[RegisterSchemaRequest]

    val senderProbe = TestProbe()

    bootstrapActor.tell(InitiateTopicBootstrap(mdRequest), senderProbe.ref)

    senderProbe.expectMsgPF() {
      case Failure(ex) => ex.getMessage shouldEqual
        "TopicBootstrapActor is in a failed state due to cause: Schema registry actor failed expectedly!"
    }

    probe.expectMsgType[RegisterSchemaRequest]

    // Check a second time to make sure we made it back to the initializing state and failed again

    bootstrapActor.tell(InitiateTopicBootstrap(mdRequest), senderProbe.ref)

    senderProbe.expectMsgPF() {
      case Failure(ex) => ex.getMessage shouldEqual
        "TopicBootstrapActor is in a failed state due to cause: Schema registry actor failed expectedly!"
    }
  }
}
