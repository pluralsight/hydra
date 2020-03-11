/*
 * Copyright (C) 2017 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hydra.kafka.producer

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.TestKit
import com.pluralsight.hydra.avro.{
  InvalidDataTypeException,
  JsonConverter,
  RequiredFieldMissingException,
  UndefinedFieldsException
}
import hydra.avro.registry.JsonToAvroConversionExceptionWithMetadata
import hydra.avro.resource.SchemaResource
import hydra.core.akka.SchemaRegistryActor.{
  FetchSchemaRequest,
  FetchSchemaResponse
}
import hydra.core.ingest.HydraRequest
import hydra.core.ingest.RequestParams._
import hydra.core.protocol.MissingMetadataException
import hydra.core.transport.AckStrategy
import hydra.kafka.producer.AvroKeyRecordFactory.NoKeySchemaFound
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source

class AvroKeyRecordFactorySpec
    extends TestKit(ActorSystem("hydra"))
    with Matchers
    with AnyFunSpecLike
    with ScalaFutures
    with BeforeAndAfterAll {

  val testSchema = new Schema.Parser()
    .parse(Source.fromResource("avro-factory-test.avsc").mkString)

  val testKeySchema = new Schema.Parser().parse("""
                                                    |{
                                                    |  "namespace": "hydra.test",
                                                    |  "type": "record",
                                                    |  "name": "Tester",
                                                    |  "fields": [
                                                    |    {
                                                    |      "name": "id",
                                                    |      "type": "string"
                                                    |    }
                                                    |  ]
                                                    |}
    """.stripMargin)

  val loader = system.actorOf(Props(new Actor() {

    override def receive: Receive = {
      case FetchSchemaRequest(name) =>
        sender ! FetchSchemaResponse(
          SchemaResource(
            1,
            1,
            testSchema
          ),
          if (name != "no-key")
            Some(
              SchemaResource(
                1,
                1,
                testKeySchema
              )
            )
          else None
        )
    }
  }))

  val factory = new AvroKeyRecordFactory(loader)

  override def afterAll = TestKit.shutdownActorSystem(system)

  override implicit val patienceConfig =
    PatienceConfig(timeout = scaled(1000 millis), interval = scaled(100 millis))

  describe("When performing validation") {
    it("handles Avro default value errors") {
      val request =
        HydraRequest(
          "123",
          """{"key": {"id":"test"}, "value":{"name":"test"}}"""
        ).withMetadata(HYDRA_SCHEMA_PARAM -> "avro-factory-test")
          .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val rec = factory.build(request)
      whenReady(rec.failed) { e =>
        val ex = e.asInstanceOf[JsonToAvroConversionExceptionWithMetadata]
        ex.getMessage should not be null
        ex.cause shouldBe an[RequiredFieldMissingException]
      }
    }

    it("handles fields not defined in the schema") {
      val request =
        HydraRequest(
          "123",
          """{"key": {"id":"test", "blah":"blah"}, "value":{"name":"test", "a":"a"}}"""
        ).withMetadata(HYDRA_SCHEMA_PARAM -> "avro-factory-test")
          .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
          .withMetadata(HYDRA_VALIDATION_STRATEGY -> "strict")
      val rec = new KafkaRecordFactories(loader).build(request)
      whenReady(rec.failed) { e =>
        val ex = e.asInstanceOf[JsonToAvroConversionExceptionWithMetadata]
        ex.cause shouldBe an[UndefinedFieldsException]
      }

    }

    it("handles Avro datatype errors") {
      val request = HydraRequest(
        "123",
        """{"key": {"id":"test"}, "value":{"name":"test", "rank":"shouldBeInt"}}"""
      ).withMetadata(HYDRA_SCHEMA_PARAM -> "avro-factory-test")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val rec = factory.build(request)
      whenReady(rec.failed) { e =>
        val ex = e.asInstanceOf[JsonToAvroConversionExceptionWithMetadata]
        ex.cause shouldBe an[InvalidDataTypeException]
      }
    }

    it("builds messages") {
      val keyJson = """{"id":"keyId"}"""
      val valueJson = """{"name":"test", "rank":10}"""
      val json =
        s"""{"key": $keyJson, "value":$valueJson}"""
      val request = HydraRequest("123", json)
        .withMetadata(HYDRA_SCHEMA_PARAM -> "avro-factory-test")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      whenReady(factory.build(request)) { msg =>
        msg.destination shouldBe "test-topic"
        msg.key.get("id") shouldBe "keyId"
        msg.key shouldBe new JsonConverter[GenericRecord](testKeySchema)
          .convert(keyJson)
        msg.valueSchema shouldBe testSchema
        msg.keySchema shouldBe testKeySchema
        msg.payload.get("name") shouldBe "test"
        msg.payload.get("rank") shouldBe 10
        msg.payload shouldBe new JsonConverter[GenericRecord](testSchema)
          .convert(valueJson)
      }
    }

    it("has the right subject when a schema is specified") {
      val request = HydraRequest(
        "123",
        """{"key": {"id":"keyId"}, "value":{"name":"test", "rank":10}}"""
      ).withMetadata(HYDRA_SCHEMA_PARAM -> "avro-factory-test")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      factory
        .getTopicAndSchemaSubject(request)
        .get
        ._2 shouldBe "avro-factory-test"
    }

    it("defaults to target as the subject") {
      val request = HydraRequest(
        "123",
        """{"key": {"id":"keyId"}, "value":{"name":"test", "rank":10}}"""
      ).withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      factory.getTopicAndSchemaSubject(request).get._2 shouldBe "test-topic"
    }

    it("throws an error if no topic is in the request") {
      val request = HydraRequest(
        "123",
        """{"key": {"id":"keyId"}, "value":{"name":"test", "rank":10}}"""
      )
      whenReady(factory.build(request).failed)(
        _ shouldBe an[MissingMetadataException]
      )
    }

    //validation
    it("returns invalid for payloads that do not conform to the schema") {
      val r = HydraRequest(
        "1",
        """{"key": {"id":"keyId"}, "value":{"failure":"test", "rank":10}}"""
      ).withMetadata(HYDRA_SCHEMA_PARAM -> "avro-factory-test.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val rec = factory.build(r)
      whenReady(rec.failed)(
        _ shouldBe a[JsonToAvroConversionExceptionWithMetadata]
      )
    }

    it("validates good avro payloads") {
      val r = HydraRequest(
        "1",
        """{"key": {"id":"keyId"}, "value":{"name":"test", "rank":10}}"""
      ).withMetadata(HYDRA_SCHEMA_PARAM -> "avro-factory-test")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      whenReady(factory.build(r)) { rec =>
        val keyRecord = new GenericRecordBuilder(testKeySchema)
          .set("id", "keyId")
          .build()
        val valueRecord = new GenericRecordBuilder(testSchema)
          .set("name", "test")
          .set("rank", 10)
          .build()
        val avroRecord = AvroKeyRecord(
          "test-topic",
          testKeySchema,
          testSchema,
          keyRecord,
          valueRecord,
          AckStrategy.NoAck
        )
        rec shouldBe avroRecord
      }
    }

    it("throws NoKeySchemaFound if key schema is not returned") {
      val request = HydraRequest(
        "123",
        """{"key": {"id":"test"}, "value":{"name":"test", "rank":10}}"""
      ).withMetadata(HYDRA_SCHEMA_PARAM -> "no-key")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val rec = factory.build(request)
      whenReady(rec.failed) { e => e shouldBe NoKeySchemaFound }
    }

  }
}
