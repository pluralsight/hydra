package hydra.kafka.ingestors

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestActors.ForwardActor
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.ConfigFactory
import hydra.avro.registry.ConfluentSchemaRegistry
import hydra.common.config.ConfigSupport
import hydra.core.ingest.HydraRequest
import hydra.core.ingest.RequestParams._
import hydra.core.protocol._
import hydra.core.transport.AckStrategy
import hydra.core.transport.AckStrategy.NoAck
import hydra.kafka.producer
import hydra.kafka.producer.{AvroRecord, AvroRecordFactory, JsonRecord}
import hydra.kafka.test.TestRecordFactory
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericRecordBuilder
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by alexsilva on 11/18/16.
  */
class KafkaIngestorSpec
  extends TestKit(ActorSystem("kafka-ingestor-spec", config = ConfigFactory.parseString("akka.actor.provider=cluster")))
    with Matchers
    with FunSpecLike
    with ImplicitSender
    with ConfigSupport
    with BeforeAndAfterAll
    with ScalaFutures
    with EmbeddedKafka {

  override def afterAll = {
    TestKit.shutdownActorSystem(system)
    EmbeddedKafka.stop()
  }

  override def beforeAll = {
    implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 8092)
    EmbeddedKafka.start()
    EmbeddedKafka.createCustomTopic("test-schema")
    EmbeddedKafka.createCustomTopic("just-a-topic")
    EmbeddedKafka.createCustomTopic("json-topic")
  }

  val schemaRegistry = ConfluentSchemaRegistry.forConfig(applicationConfig)
  val registryClient = schemaRegistry.registryClient
  val probe = TestProbe()
  val supervisor = TestProbe()

  val kafkaProducer = system.actorOf(Props(new ForwardActor(probe.ref)), "kafka_producer")

  val kafkaIngestor = probe.childActorOf(Props[KafkaIngestor])


  val KAFKA = "kafka_ingestor"

  val schema =
    """{
      |     "type": "record",
      |     "namespace": "com.example",
      |     "name": "FullName",
      |     "fields": [
      |       { "name": "first", "type": "string" },
      |       { "name": "last", "type": "string" }
      |     ]
      |} """.stripMargin

  val schema2 =
    """{
      |     "type": "record",
      |     "namespace": "com.example",
      |     "name": "FullName2",
      |     "fields": [
      |       { "name": "first", "type": "string" },
      |       { "name": "last", "type": "string" }
      |     ]
      |} """.stripMargin

  val avroSchema = new Schema.Parser().parse(schema)

  val record = new GenericRecordBuilder(avroSchema).set("first", "hydra").set("last", "hydra").build()
  val ar = producer.AvroRecord("test-schema", avroSchema, None, record, AckStrategy.NoAck)

  val json = """{"first":"hydra","last":"hydra"}"""

  registryClient.register("test-schema-value", new Parser().parse(schema))
  registryClient.register("test-schema2-value", new Parser().parse(schema2))

  describe("when using the KafkaIngestor") {
    it("joins") {
      val request = HydraRequest(
        "123",
        "someString", None,
        Map(HYDRA_INGESTOR_PARAM -> KAFKA, HYDRA_KAFKA_TOPIC_PARAM -> "topic"))
      kafkaIngestor ! Publish(request)
      expectMsg(Join)
    }

    it("is invalid when there is no topic") {
      val request = HydraRequest(
        "123",
        "someString", None,
        Map(HYDRA_INGESTOR_PARAM -> KAFKA, HYDRA_RECORD_FORMAT_PARAM -> "json"))
      kafkaIngestor ! Validate(request)
      expectMsgType[InvalidRequest]
    }

    it("ingests") {
      val request = HydraRequest(
        "123",
        """{"first":"Roar","last":"King"}""", None,
        Map(HYDRA_INGESTOR_PARAM -> KAFKA, HYDRA_KAFKA_TOPIC_PARAM -> "test-schema"))
      whenReady(TestRecordFactory.build(request)) { record =>
        kafkaIngestor ! Ingest(record, NoAck)
        probe.expectMsg(Produce(record, self, NoAck))
      }
    }
  }

  val loader = TestProbe()
  val avroRecordFactory = new AvroRecordFactory(loader.ref)

  it("is invalid if it can't find the schema") {
    val request = HydraRequest(
      "213",
      "someString", None,
      Map(HYDRA_INGESTOR_PARAM -> KAFKA, HYDRA_KAFKA_TOPIC_PARAM -> "avro-topic"))
    kafkaIngestor ! Validate(request)
    expectMsgType[InvalidRequest]
  }

  it("is valid with no schema if the topic can be resolved to a string") {
    val request = HydraRequest(
      "123",
      json, None,
      Map(HYDRA_INGESTOR_PARAM -> KAFKA, HYDRA_KAFKA_TOPIC_PARAM -> "test-schema"))
    kafkaIngestor ! Validate(request)
    avroRecordFactory.getTopicAndSchemaSubject(request).get._2 shouldBe "test-schema"
    expectMsg(ValidRequest(ar))
  }

  it("is valid when a schema name overrides the topic name") {
    val request = HydraRequest(
      "123",
      json, None,
      Map(HYDRA_INGESTOR_PARAM -> KAFKA, HYDRA_KAFKA_TOPIC_PARAM -> "just-a-topic", HYDRA_SCHEMA_PARAM -> "test-schema"))
    avroRecordFactory.getTopicAndSchemaSubject(request).get._2 shouldBe "test-schema"
    kafkaIngestor ! Validate(request)
    val ar = AvroRecord("just-a-topic", avroSchema, None, record, AckStrategy.NoAck)
    expectMsg(ValidRequest(ar))
  }
  it("is valid if schema can't be found, but json is allowed") {
    val request = HydraRequest("123", json, None,
      Map(HYDRA_INGESTOR_PARAM -> KAFKA, HYDRA_RECORD_FORMAT_PARAM -> "json", HYDRA_KAFKA_TOPIC_PARAM -> "json-topic"))
    kafkaIngestor ! Validate(request)
    val node = new ObjectMapper().reader().readTree("""{"first":"hydra","last":"hydra"}""")
    expectMsg(ValidRequest(JsonRecord("json-topic", None, node, AckStrategy.NoAck)))
  }

  it("is invalid if schema exists, but topic doesn't") {
    val request = HydraRequest(
      "123",
      json, None,
      Map(HYDRA_INGESTOR_PARAM -> KAFKA, HYDRA_KAFKA_TOPIC_PARAM -> "test-schema2"))
    avroRecordFactory.getTopicAndSchemaSubject(request).get._2 shouldBe "test-schema2"
    kafkaIngestor ! Validate(request)
    expectMsgPF() {
      case InvalidRequest(ex) => ex shouldBe an[IllegalArgumentException]
    }
  }

}
