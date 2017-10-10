package hydra.kafka.ingestors

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.fasterxml.jackson.databind.ObjectMapper
import hydra.avro.registry.ConfluentSchemaRegistry
import hydra.common.config.ConfigSupport
import hydra.core.ingest.RequestParams._
import hydra.core.ingest.HydraRequest
import hydra.core.protocol._
import hydra.kafka.test.TestRecordFactory
import hydra.core.transport.{AckStrategy, DeliveryStrategy}
import hydra.kafka.producer.{AvroRecord, AvroRecordFactory, JsonRecord}
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericRecordBuilder
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 11/18/16.
  */
class KafkaIngestorSpec extends TestKit(ActorSystem("hydra-test")) with Matchers with FunSpecLike
  with ImplicitSender with ConfigSupport {

  val schemaRegistry = ConfluentSchemaRegistry.forConfig(applicationConfig)
  val registryClient = schemaRegistry.registryClient
  val kafkaProducer = TestProbe("kafka_producer_actor")

  val transport = kafkaProducer.childActorOf(Props[KafkaIngestor])

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

  val avroSchema = new Schema.Parser().parse(schema)

  val record = new GenericRecordBuilder(avroSchema).set("first", "hydra").set("last", "hydra").build()
  val ar = AvroRecord("test-schema", avroSchema, None, record, DeliveryStrategy.AtMostOnce, AckStrategy.None)

  val json = """{"first":"hydra","last":"hydra"}"""

  registryClient.register("test-schema-value", new Parser().parse(schema))

  describe("when using the KafkaIngestor") {
    it("joins") {
      val request = HydraRequest(123,
        "someString",
        Map(HYDRA_INGESTOR_PARAM -> KAFKA, HYDRA_KAFKA_TOPIC_PARAM -> "topic")
      )
      transport ! Publish(request)
      expectMsg(Join)
    }

    it("is invalid when there is no topic") {
      val request = HydraRequest(123,
        "someString",
        Map(HYDRA_INGESTOR_PARAM -> KAFKA, HYDRA_RECORD_FORMAT_PARAM -> "json")
      )
      transport ! Validate(request)
      expectMsgType[InvalidRequest]
    }

    it("ingests") {
      val request = HydraRequest(1234,
        """{"first":"Roar","last":"King"}""",
        Map(HYDRA_INGESTOR_PARAM -> KAFKA, HYDRA_KAFKA_TOPIC_PARAM -> "test-schema")
      )
      transport ! Ingest(TestRecordFactory.build(request).get)
      expectMsg(IngestorCompleted)
    }
  }


  it("is invalid if it can't find the schema") {
    val request = HydraRequest(213,
      "someString",
      Map(HYDRA_INGESTOR_PARAM -> KAFKA, HYDRA_KAFKA_TOPIC_PARAM -> "avro-topic")
    )
    transport ! Validate(request)
    expectMsgType[InvalidRequest]
  }

  it("is valid with no schema if the topic can be resolved to a string") {
    val request = HydraRequest(123,
      json,
      Map(HYDRA_INGESTOR_PARAM -> KAFKA, HYDRA_KAFKA_TOPIC_PARAM -> "test-schema")
    )
    transport ! Validate(request)
    AvroRecordFactory.getSubject(request) shouldBe "test-schema"
    expectMsg(ValidRequest(ar))
  }

  it("is valid when a schema name overrides the topic name") {
    val request = HydraRequest(123,
      json,
      Map(HYDRA_INGESTOR_PARAM -> KAFKA, HYDRA_KAFKA_TOPIC_PARAM -> "just-a-topic", HYDRA_SCHEMA_PARAM -> "test-schema")
    )
    AvroRecordFactory.getSubject(request) shouldBe "test-schema"
    transport ! Validate(request)
    val ar = AvroRecord("just-a-topic", avroSchema, None, record, DeliveryStrategy.AtMostOnce, AckStrategy.None)
    expectMsg(ValidRequest(ar))
  }
  it("is valid if schema can't be found, but json is allowed") {
    val request = HydraRequest(123, json,
      Map(HYDRA_INGESTOR_PARAM -> KAFKA, HYDRA_RECORD_FORMAT_PARAM -> "json", HYDRA_KAFKA_TOPIC_PARAM -> "json-topic"))
    transport ! Validate(request)
    val node = new ObjectMapper().reader().readTree("""{"first":"hydra","last":"hydra"}""")
    expectMsg(ValidRequest(JsonRecord("json-topic", None, node)))
  }

}