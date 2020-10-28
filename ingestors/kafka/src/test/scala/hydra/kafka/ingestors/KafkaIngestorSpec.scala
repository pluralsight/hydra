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
import org.apache.avro.generic.{GenericData, GenericRecordBuilder}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by alexsilva on 11/18/16.
  */
class KafkaIngestorSpec
    extends TestKit(ActorSystem("kafka-ingestor-spec"))
    with Matchers
    with AnyFunSpecLike
    with ConfigSupport
    with BeforeAndAfterAll
    with ScalaFutures
    with EmbeddedKafka {

  import KafkaIngestorSpec._

  val schemaRegistry = ConfluentSchemaRegistry.forConfig(applicationConfig)

  val registryClient = schemaRegistry.registryClient

  val loader = TestProbe()

  val avroRecordFactory = new AvroRecordFactory(loader.ref)

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

  val record: GenericData.Record = new GenericRecordBuilder(avroSchema)
    .set("first", "hydra")
    .set("last", "hydra")
    .build()

  val ar = producer.AvroRecord(
    "test-schema",
    avroSchema,
    None,
    record,
    AckStrategy.NoAck
  )

  val json = """{"first":"hydra","last":"hydra"}"""

  // Ingestor uses an actor selection so this class has to be instantiated beforehand so it can be resolved
  val kafkaProducerProbe = TestProbe()

  val kafkaProducer = system.actorOf(
    Props(new ForwardActor(kafkaProducerProbe.ref)),
    "kafka_producer"
  )

  override def beforeAll = {
    implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 8042)
    EmbeddedKafka.start()
    EmbeddedKafka.createCustomTopic("test-schema")
    EmbeddedKafka.createCustomTopic("just-a-topic")
    EmbeddedKafka.createCustomTopic("json-topic")
    registryClient.register("test-schema-value", new Parser().parse(schema))
    registryClient.register("test-schema2-value", new Parser().parse(schema2))
  }

  override def afterAll = {
    TestKit.shutdownActorSystem(system)
    EmbeddedKafka.stop()
  }

  describe("when using the KafkaIngestor") {
    it("joins") {
      val kafkaIngestor = system.actorOf(Props[KafkaIngestor])
      val probe = TestProbe()

      val request = HydraRequest(
        "123",
        "someString",
        None,
        Map(
          HYDRA_INGESTOR_PARAM -> KafkaIngestorName,
          HYDRA_KAFKA_TOPIC_PARAM -> "topic"
        )
      )
      kafkaIngestor.tell(Publish(request), probe.ref)
      probe.expectMsg(Join)
    }

    it("is invalid when there is no topic") {
      val kafkaIngestor = system.actorOf(Props[KafkaIngestor])
      val probe = TestProbe()

      val request = HydraRequest(
        "123",
        "someString",
        None,
        Map(
          HYDRA_INGESTOR_PARAM -> KafkaIngestorName,
          HYDRA_RECORD_FORMAT_PARAM -> "json"
        )
      )
      kafkaIngestor.tell(Validate(request), probe.ref)
      probe.expectMsgType[InvalidRequest]
    }

    it("ingests") {
      val kafkaIngestor = system.actorOf(Props[KafkaIngestor])
      val probe = TestProbe()

      val request = HydraRequest(
        "123",
        """{"first":"Roar","last":"King"}""",
        None,
        Map(
          HYDRA_INGESTOR_PARAM -> KafkaIngestorName,
          HYDRA_KAFKA_TOPIC_PARAM -> "test-schema"
        )
      )
      whenReady(TestRecordFactory.build(request)) { record =>
        kafkaIngestor.tell(Ingest(record, NoAck), probe.ref)
        kafkaProducerProbe.expectMsg(Produce(record, probe.ref, NoAck))
      }
    }
  }

  it("is invalid if it can't find the schema") {
    val kafkaIngestor = system.actorOf(Props[KafkaIngestor])
    val probe = TestProbe()

    val request = HydraRequest(
      "213",
      "someString",
      None,
      Map(
        HYDRA_INGESTOR_PARAM -> KafkaIngestorName,
        HYDRA_KAFKA_TOPIC_PARAM -> "avro-topic"
      )
    )
    kafkaIngestor.tell(Validate(request), probe.ref)
    probe.expectMsgType[InvalidRequest]
  }

  it("is valid with no schema if the topic can be resolved to a string") {
    val kafkaIngestor = system.actorOf(Props[KafkaIngestor])
    val probe = TestProbe()

    val request = HydraRequest(
      "123",
      json,
      None,
      Map(
        HYDRA_INGESTOR_PARAM -> KafkaIngestorName,
        HYDRA_KAFKA_TOPIC_PARAM -> "test-schema"
      )
    )
    kafkaIngestor.tell(Validate(request), probe.ref)
    avroRecordFactory
      .getTopicAndSchemaSubject(request)
      .get
      ._2 shouldBe "test-schema"
    probe.expectMsg(ValidRequest(ar))
  }

  it("is valid when a schema name overrides the topic name") {
    val kafkaIngestor = system.actorOf(Props[KafkaIngestor])
    val probe = TestProbe()

    val request = HydraRequest(
      "123",
      json,
      None,
      Map(
        HYDRA_INGESTOR_PARAM -> KafkaIngestorName,
        HYDRA_KAFKA_TOPIC_PARAM -> "just-a-topic",
        HYDRA_SCHEMA_PARAM -> "test-schema"
      )
    )
    avroRecordFactory
      .getTopicAndSchemaSubject(request)
      .get
      ._2 shouldBe "test-schema"
    kafkaIngestor.tell(Validate(request), probe.ref)
    val ar =
      AvroRecord("just-a-topic", avroSchema, None, record, AckStrategy.NoAck)
    probe.expectMsg(ValidRequest(ar))
  }

  it("is valid if schema can't be found, but json is allowed") {
    val kafkaIngestor = system.actorOf(Props[KafkaIngestor])
    val probe = TestProbe()

    val request = HydraRequest(
      "123",
      json,
      None,
      Map(
        HYDRA_INGESTOR_PARAM -> KafkaIngestorName,
        HYDRA_RECORD_FORMAT_PARAM -> "json",
        HYDRA_KAFKA_TOPIC_PARAM -> "json-topic"
      )
    )
    kafkaIngestor.tell(Validate(request), probe.ref)
    val node = new ObjectMapper()
      .reader()
      .readTree("""{"first":"hydra","last":"hydra"}""")
    probe.expectMsg(
      ValidRequest(JsonRecord("json-topic", None, node, AckStrategy.NoAck))
    )
  }

//  it("is invalid if schema exists, but topic doesn't") {
//    val kafkaIngestor = system.actorOf(Props[KafkaIngestor])
//    val probe = TestProbe()
//
//    val request = HydraRequest(
//      "123",
//      json, None,
//      Map(HYDRA_INGESTOR_PARAM -> KafkaIngestorName, HYDRA_KAFKA_TOPIC_PARAM -> "test-schema2"))
//    avroRecordFactory.getTopicAndSchemaSubject(request).get._2 shouldBe "test-schema2"
//    kafkaIngestor.tell(Validate(request), probe.ref)
//    probe.expectMsgPF() {
//      case InvalidRequest(ex) => ex shouldBe an[IllegalArgumentException]
//    }
//  }

}

object KafkaIngestorSpec {
  val KafkaIngestorName = "kafka_ingestor"
}
