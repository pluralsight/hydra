package hydra.kafka.services

import java.util.UUID

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorSystem, Props}
import akka.pattern.pipe
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import hydra.avro.resource.SchemaResource
import hydra.core.akka.SchemaRegistryActor._
import hydra.core.marshallers.TopicMetadataRequest
import hydra.core.protocol.{Ingest, IngestorCompleted}
import hydra.core.transport.AckStrategy
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import hydra.kafka.model.TopicMetadata
import hydra.kafka.producer.AvroRecord
import hydra.kafka.services.StreamsManagerActor.{
  GetMetadata,
  GetMetadataResponse
}
import hydra.kafka.services.TopicBootstrapActor._
import net.manub.embeddedkafka.{
  EmbeddedKafka,
  EmbeddedKafkaConfig,
  KafkaUnavailableException
}
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.StringSerializer
import org.joda.time.DateTime
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.BeforeAndAfterAll
import spray.json._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.io.Source

class TopicBootstrapActorSpec
    extends TestKit(ActorSystem("topic-bootstrap-actor-spec"))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockFactory
    with EmbeddedKafka
    with HydraKafkaJsonSupport
    with Eventually {

  import hydra.kafka.services.ErrorMessages._

  implicit val ec = system.dispatcher

  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(
    kafkaPort = 8012,
    zooKeeperPort = 3111,
    customBrokerProperties = Map("auto.create.topics.enable" -> "false")
  )

  override def beforeAll(): Unit = {
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  val config = ConfigFactory.load().getConfig("hydra_kafka.bootstrap-config")

  val topicMetadataJson =
    Source.fromResource("HydraMetadataTopic.avsc").mkString

  val testSchemaResource =
    SchemaResource(1, 1, new Schema.Parser().parse(topicMetadataJson))

  def fixture(
      key: String,
      kafkaShouldFail: Boolean = false,
      schemaRegistryShouldFail: Boolean = false
  ) = {
    val probe = TestProbe()

    val schemaRegistryActor = system.actorOf(
      Props(
        new Actor {
          override def receive: Receive = {
            case msg: FetchSchemaRequest =>
              sender ! FetchSchemaResponse(testSchemaResource, None)
              probe.ref forward msg

            case msg: RegisterSchemaRequest =>
              if (schemaRegistryShouldFail) {
                sender ! Failure(
                  new Exception("Schema registry actor failed expectedly!")
                )
                probe.ref forward msg
              } else {
                sender ! RegisterSchemaResponse(testSchemaResource)
                probe.ref forward msg
              }
          }
        }
      ),
      s"test-schema-registry-$key"
    )

    val kafkaIngestor = system.actorOf(
      Props(
        new Actor {
          override def receive = {
            case msg: Ingest[_, _] =>
              probe.ref forward msg
              if (kafkaShouldFail) {
                Future.failed(
                  new Exception("Kafka ingestor failed expectedly!")
                ) pipeTo sender
              } else {
                sender ! IngestorCompleted
              }
          }
        }
      ),
      s"kafka_ingestor_$key"
    )

    (probe, schemaRegistryActor, kafkaIngestor)
  }

  "A TopicBootstrapActor" should "process metadata and send an Ingest message to the kafka ingestor" in {

    val mdRequest =
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
                      |	  "name": "testsubject1",
                      |	  "type": "record",
                      |	  "version": 1,
                      |	  "fields": [
                      |	    {
                      |	      "name": "testField",
                      |	      "type": "string"
                      |	    }
                      |	  ]
                      |	}
                      |}""".stripMargin.parseJson
        .convertTo[TopicMetadataRequest]

    val (probe, schemaRegistryActor, kafkaIngestor) = fixture("test1")

    val bootstrapActor = system.actorOf(
      TopicBootstrapActor.props(
        schemaRegistryActor,
        system.actorSelection("/user/kafka_ingestor_test1"),
        Props(new MockStreamsManagerActor())
      )
    )

    probe.expectMsgType[RegisterSchemaRequest]

    bootstrapActor ! InitiateTopicBootstrap(mdRequest)

    probe.receiveWhile(messages = 2) {
      case RegisterSchemaRequest(schemaJson) =>
        schemaJson should
          include("testsubject1")
      case FetchSchemaRequest(schemaName) =>
        schemaName shouldEqual "_hydra.metadata.topic"
    }

    probe.expectMsgPF() {
      case Ingest(msg: AvroRecord, ack) =>
        msg.payload.getSchema.getName shouldBe "topic"
        ack shouldBe AckStrategy.Replicated
    }
  }

  it should "respond with the error that caused the failed actor state" in {
    val mdRequest =
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
                      |	  "name": "testsubject2",
                      |	  "type": "record",
                      |	  "version": 1,
                      |	  "fields": [
                      |	    {
                      |	      "name": "testField",
                      |	      "type": "string"
                      |	    }
                      |	  ]
                      |	}
                      |}""".stripMargin.parseJson
        .convertTo[TopicMetadataRequest]

    val (probe, schemaRegistryActor, _) =
      fixture("test2", schemaRegistryShouldFail = true)

    val bootstrapActor = system.actorOf(
      TopicBootstrapActor.props(
        schemaRegistryActor,
        system.actorSelection("/user/kafka_ingestor_test2"),
        Props(new MockStreamsManagerActor())
      )
    )

    probe.expectMsgType[RegisterSchemaRequest]

    val senderProbe = TestProbe()

    bootstrapActor.tell(InitiateTopicBootstrap(mdRequest), senderProbe.ref)

    senderProbe.expectMsgPF() {
      case Failure(ex) =>
        ex.getMessage shouldEqual
          "TopicBootstrapActor is in a failed state due to cause: Schema registry actor failed expectedly!"
    }
  }

  it should "respond with the appropriate metadata failure message" in {

    val mdRequest =
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
                      |	  "namespace": "exp....",
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
                      |}""".stripMargin.parseJson
        .convertTo[TopicMetadataRequest]

    val (probe, schemaRegistryActor, kafkaIngestor) = fixture("test3")

    val bootstrapActor = system.actorOf(
      TopicBootstrapActor.props(
        schemaRegistryActor,
        system.actorSelection("/user/kafka_ingestor_test3"),
        Props(new MockStreamsManagerActor())
      )
    )

    probe.expectMsgType[RegisterSchemaRequest]

    val senderProbe = TestProbe()

    bootstrapActor.tell(InitiateTopicBootstrap(mdRequest), senderProbe.ref)

    senderProbe.expectMsgPF() {
      case BootstrapFailure(reasons) =>
        reasons should contain(BadTopicFormatError)
    }
  }

  it should "respond with the appropriate failure when the KafkaIngestor returns an exception" in {
    val mdRequest =
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
                      |	  "name": "testsubject4",
                      |	  "type": "record",
                      |	  "version": 1,
                      |	  "fields": [
                      |	    {
                      |	      "name": "testField",
                      |	      "type": "string"
                      |	    }
                      |	  ]
                      |	}
                      |}""".stripMargin.parseJson
        .convertTo[TopicMetadataRequest]

    val (probe, schemaRegistryActor, _) =
      fixture("test4", kafkaShouldFail = true)

    val bootstrapActor = system.actorOf(
      TopicBootstrapActor.props(
        schemaRegistryActor,
        system.actorSelection("/user/kafka_ingestor_test4"),
        Props(new MockStreamsManagerActor())
      )
    )

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
                       |	  "name": "testsubject5",
                       |	  "type": "record",
                       |	  "version": 1,
                       |	  "fields": [
                       |	    {
                       |	      "name": "testField",
                       |	      "type": "string"
                       |	    }
                       |	  ]
                       |	}
                       |}""".stripMargin.parseJson
      .convertTo[TopicMetadataRequest]

    val (probe, schemaRegistryActor, _) = fixture("test5")

    val bootstrapActor = system.actorOf(
      TopicBootstrapActor.props(
        schemaRegistryActor,
        system.actorSelection("/user/kafka_ingestor_test5"),
        Props(new MockStreamsManagerActor())
      )
    )

    probe.expectMsgType[RegisterSchemaRequest]

    val senderProbe = TestProbe()

    bootstrapActor.tell(InitiateTopicBootstrap(mdRequest), senderProbe.ref)

    probe.receiveWhile(messages = 2) {
      case RegisterSchemaRequest(schemaJson) =>
        schemaJson should
          include("testsubject5")
      case FetchSchemaRequest(schemaName) =>
        schemaName shouldEqual "_hydra.metadata.topic"
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
                       |	  "name": "testsubject6",
                       |	  "type": "record",
                       |	  "version": 1,
                       |	  "fields": [
                       |	    {
                       |	      "name": "testField",
                       |	      "type": "string"
                       |	    }
                       |	  ]
                       |	}
                       |}""".stripMargin.parseJson
      .convertTo[TopicMetadataRequest]

    val testConfig = ConfigFactory.parseString("""
        |{
        |  partitions = 3
        |  replication-factor = 3
        |  timeout = 300
        |  failure-retry-millis = 3000
        |}
      """.stripMargin)

    val (probe, schemaRegistryActor, _) = fixture("test6")

    val bootstrapActor = system.actorOf(
      TopicBootstrapActor.props(
        schemaRegistryActor,
        system.actorSelection("/user/kafka_ingestor_test6"),
        Props(new MockStreamsManagerActor()),
        Some(testConfig)
      )
    )

    probe.expectMsgType[RegisterSchemaRequest]

    val senderProbe = TestProbe()

    bootstrapActor.tell(InitiateTopicBootstrap(mdRequest), senderProbe.ref)

    probe.receiveWhile(messages = 2) {
      case RegisterSchemaRequest(schemaJson) =>
        schemaJson should
          include("testsubject6")
      case FetchSchemaRequest(schemaName) =>
        schemaName shouldEqual "_hydra.metadata.topic"
    }

    probe.expectMsgPF() {
      case Ingest(msg: AvroRecord, ack) =>
        msg.payload.getSchema.getName shouldBe "topic"
        ack shouldBe AckStrategy.Replicated
    }

    senderProbe.expectMsgPF() {
      case BootstrapFailure(reasons) =>
        reasons.nonEmpty shouldBe true
        reasons.head should include(
          "Replication factor: 3 larger than available brokers: 1."
        )
    }
  }

  it should "not fail due to a topic already existing" in {
    val subject = "exp.dataplatform.testsubject7"

    val mdRequest = s"""{
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
                       |	  "name": "testsubject7",
                       |	  "type": "record",
                       |	  "version": 1,
                       |	  "fields": [
                       |	    {
                       |	      "name": "testField",
                       |	      "type": "string"
                       |	    }
                       |	  ]
                       |	}
                       |}""".stripMargin.parseJson
      .convertTo[TopicMetadataRequest]

    val (probe, schemaRegistryActor, _) = fixture("test7")

    val bootstrapActor = system.actorOf(
      TopicBootstrapActor.props(
        schemaRegistryActor,
        system.actorSelection("/user/kafka_ingestor_test7"),
        Props(new MockStreamsManagerActor())
      )
    )

    probe.expectMsgType[RegisterSchemaRequest]

    val senderProbe = TestProbe()

    bootstrapActor.tell(InitiateTopicBootstrap(mdRequest), senderProbe.ref)

    probe.receiveWhile(messages = 2) {
      case RegisterSchemaRequest(schemaJson) =>
        schemaJson should
          include("testsubject7")
      case FetchSchemaRequest(schemaName) =>
        schemaName shouldEqual "_hydra.metadata.topic"
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

  it should "get all streams" in {
    val (probe, schemaRegistryActor, _) =
      fixture("test-get-stream", schemaRegistryShouldFail = false)

    val bootstrapActor = system.actorOf(
      TopicBootstrapActor.props(
        schemaRegistryActor,
        system.actorSelection("kafka_ingestor_test-get-stream"),
        Props(new MockStreamsManagerActor())
      )
    )

    probe.expectMsgType[RegisterSchemaRequest]

    val senderProbe = TestProbe()

    bootstrapActor.tell(GetStreams(None), senderProbe.ref)

    senderProbe.expectMsgPF() {
      case GetStreamsResponse(md) =>
        md.exists(m => m.subject == "test-md-subject") shouldBe true
    }
  }

  it should "get a stream by its subject" in {
    val (probe, schemaRegistryActor, _) = fixture("test-get-stream-subject")

    val bootstrapActor = system.actorOf(
      TopicBootstrapActor.props(
        schemaRegistryActor,
        system.actorSelection("kafka_ingestor_test-get-stream-subject"),
        Props(new MockStreamsManagerActor())
      )
    )

    probe.expectMsgType[RegisterSchemaRequest]

    val senderProbe = TestProbe()

    bootstrapActor.tell(GetStreams(Some("unknown")), senderProbe.ref)

    senderProbe.expectMsg(GetStreamsResponse(Seq.empty))

    bootstrapActor.tell(GetStreams(Some("test-md-subject")), senderProbe.ref)

    senderProbe.expectMsgPF() {
      case GetStreamsResponse(md) =>
        md.exists(m => m.subject == "test-md-subject") shouldBe true
    }
  }

  it should "retry after a configurable interval when the schema registration fails" in {
    val mdRequest =
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
                      |	  "name": "testsubject8",
                      |	  "type": "record",
                      |	  "version": 1,
                      |	  "fields": [
                      |	    {
                      |	      "name": "testField",
                      |	      "type": "string"
                      |	    }
                      |	  ]
                      |	}
                      |}""".stripMargin.parseJson
        .convertTo[TopicMetadataRequest]

    val (probe, schemaRegistryActor, _) =
      fixture("test8", schemaRegistryShouldFail = true)

    val bootstrapActor = system.actorOf(
      TopicBootstrapActor.props(
        schemaRegistryActor,
        system.actorSelection("/user/kafka_ingestor_test8"),
        Props(new MockStreamsManagerActor())
      )
    )
    probe.expectMsgType[RegisterSchemaRequest]

    val senderProbe = TestProbe()

    bootstrapActor.tell(InitiateTopicBootstrap(mdRequest), senderProbe.ref)

    senderProbe.expectMsgPF() {
      case Failure(ex) =>
        ex.getMessage shouldEqual
          "TopicBootstrapActor is in a failed state due to cause: Schema registry actor failed expectedly!"
    }

    probe.expectMsgType[RegisterSchemaRequest](max = 10 seconds)

    // Check a second time to make sure we made it back to the initializing state and failed again

    bootstrapActor.tell(InitiateTopicBootstrap(mdRequest), senderProbe.ref)

    senderProbe.expectMsgPF() {
      case Failure(ex) =>
        ex.getMessage shouldEqual
          "TopicBootstrapActor is in a failed state due to cause: Schema registry actor failed expectedly!"
    }
  }

  it should "create a topic if a hydra key exists and the stream type is historic" in {

    val mdRequest = s"""{
                         |	"streamType": "History",
                         |  "derived": false,
                         |	"dataClassification": "Public",
                         |	"dataSourceOwner": "BARTON",
                         |	"contact": "slackity slack dont talk back",
                         |	"psDataLake": false,
                         |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
                         |	"notes": "here are some notes topkek",
                         |	"schema": {
                         |	  "namespace": "exp.dataplatform",
                         |	  "name": "testsbject5",
                         |    "hydra.key": "testField",
                         |	  "type": "record",
                         |	  "version": 1,
                         |	  "fields": [
                         |	    {
                         |	      "name": "testField",
                         |	      "type": "string"
                         |	    }
                         |	  ]
                         |	}
                         |}""".stripMargin.parseJson
      .convertTo[TopicMetadataRequest]

    val (probe, schemaRegistryActor, _) = fixture("test12")

    val bootstrapActor = system.actorOf(
      TopicBootstrapActor.props(
        schemaRegistryActor,
        system.actorSelection("/user/kafka_ingestor_test12"),
        Props(new MockStreamsManagerActor())
      )
    )

    probe.expectMsgType[RegisterSchemaRequest]

    val senderProbe = TestProbe()

    bootstrapActor.tell(InitiateTopicBootstrap(mdRequest), senderProbe.ref)

    val expectedMessage = "message"
    val consumeSubject = "exp.dataplatform.testsbject5"

    //need to publish a KEY and VALUE here
    publishToKafka(consumeSubject, expectedMessage, expectedMessage)(
      config = embeddedKafkaConfig,
      new StringSerializer(),
      new StringSerializer()
    )
    consumeFirstStringMessageFrom(consumeSubject) shouldEqual expectedMessage
  }

  it should "create a topic if hydra.key is not present" in {

    val mdRequest = s"""{
                       |	"streamType": "History",
                       |  "derived": false,
                       |	"dataClassification": "Public",
                       |	"dataSourceOwner": "BARTON",
                       |	"contact": "slackity slack dont talk back",
                       |	"psDataLake": false,
                       |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
                       |	"notes": "here are some notes topkek",
                       |	"schema": {
                       |	  "namespace": "exp.dataplatform",
                       |	  "name": "testsbject6",
                       |	  "type": "record",
                       |	  "version": 1,
                       |	  "fields": [
                       |	    {
                       |	      "name": "testField",
                       |	      "type": "string"
                       |	    }
                       |	  ]
                       |	}
                       |}""".stripMargin.parseJson
      .convertTo[TopicMetadataRequest]

    val (probe, schemaRegistryActor, _) = fixture("test13")

    val bootstrapActor = system.actorOf(
      TopicBootstrapActor.props(
        schemaRegistryActor,
        system.actorSelection("/user/kafka_ingestor_test13"),
        Props(new MockStreamsManagerActor())
      )
    )

    probe.expectMsgType[RegisterSchemaRequest]

    val senderProbe = TestProbe()

    bootstrapActor.tell(InitiateTopicBootstrap(mdRequest), senderProbe.ref)

    val expectedMessage = "message"
    val consumeSubject = "exp.dataplatform.testsbject6"

    //need to publish a KEY and VALUE here
    publishToKafka(consumeSubject, expectedMessage, expectedMessage)(
      config = embeddedKafkaConfig,
      new StringSerializer(),
      new StringSerializer()
    )
    consumeFirstStringMessageFrom(consumeSubject) shouldEqual expectedMessage
  }
}

class MockStreamsManagerActor extends Actor {

  val tm = TopicMetadata(
    "test-md-subject",
    1,
    "entity",
    false,
    None,
    "private",
    "alex",
    None,
    None,
    UUID.randomUUID(),
    DateTime.now()
  )

  override def receive: Receive = {
    case GetMetadata =>
      sender ! GetMetadataResponse(Map("test-md-subject" -> tm))
  }
}
