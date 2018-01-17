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

import java.io.File

import com.pluralsight.hydra.avro.{InvalidDataTypeException, JsonConverter, RequiredFieldMissingException, UndefinedFieldsException}
import hydra.avro.JsonToAvroConversionExceptionWithMetadata
import hydra.core.ingest.RequestParams._
import hydra.core.ingest.{HydraRequest, InvalidRequestException}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpecLike, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source

/**
  * Created by alexsilva on 1/11/17.
  */
class AvroRecordFactorySpec extends Matchers with FunSpecLike with ScalaFutures {

  val schema = Thread.currentThread().getContextClassLoader.getResource("schema.avsc").getFile

  describe("When performing validation") {
    it("handles Avro default value errors") {
      val request = HydraRequest("123","""{"name":"test"}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val rec = AvroRecordFactory.build(request)
      whenReady(rec.failed) { e =>
        val ex = e.asInstanceOf[JsonToAvroConversionExceptionWithMetadata]
        ex.cause shouldBe an[RequiredFieldMissingException]
      }
    }

    it("handles fields not defined in the schema") {
      val request = HydraRequest("123","""{"name":"test","rank":1,"new-field":"new"}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
        .withMetadata(HYDRA_VALIDATION_STRATEGY -> "strict")
      val rec = KafkaRecordFactories.build(request)
      whenReady(rec.failed) { e =>
        val ex = e.asInstanceOf[JsonToAvroConversionExceptionWithMetadata]
        ex.cause shouldBe an[UndefinedFieldsException]
      }

    }

    it("handles Avro datatype errors") {
      val request = HydraRequest("123","""{"name":"test", "rank":"booyah"}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val rec = AvroRecordFactory.build(request)
      whenReady(rec.failed) { e =>
        val ex = e.asInstanceOf[JsonToAvroConversionExceptionWithMetadata]
        ex.cause shouldBe an[InvalidDataTypeException]
      }
    }

    it("builds keyless messages") {
      val avroSchema = new Schema.Parser().parse(new File(schema))
      val json = """{"name":"test", "rank":10}"""
      val request = HydraRequest("123", json)
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      whenReady(AvroRecordFactory.build(request)) { msg =>
        msg.destination shouldBe "test-topic"
        msg.key shouldBe None
        msg.schema shouldBe avroSchema
        msg.payload.get("name") shouldBe "test"
        msg.payload.get("rank") shouldBe 10
        msg.payload shouldBe new JsonConverter[GenericRecord](avroSchema).convert(json)
      }
    }

    it("builds keyed messages") {
      val avroSchema = new Schema.Parser().parse(new File(schema))
      val json = """{"name":"test", "rank":10}"""
      val request = HydraRequest("123", json)
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_RECORD_KEY_PARAM -> "{$.name}")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      whenReady(AvroRecordFactory.build(request)) { msg =>
        msg.destination shouldBe "test-topic"
        msg.schema shouldBe avroSchema
        msg.payload.get("name") shouldBe "test"
        msg.payload.get("rank") shouldBe 10
        msg.key shouldBe Some("test")
        msg.payload shouldBe new JsonConverter[GenericRecord](avroSchema).convert(json)
      }
    }

    it("has the right subject when a schema is specified") {
      val request = HydraRequest("123","""{"name":"test", "rank":10}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      AvroRecordFactory.getSubject(request) shouldBe "classpath:schema.avsc"
    }

    it("defaults to target as the subject") {
      val request = HydraRequest("123","""{"name":"test", "rank":10}""")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      AvroRecordFactory.getSubject(request) shouldBe "test-topic"
    }

    it("throws an error if no topic is in the request") {
      val request = HydraRequest("123","""{"name":test"}""")
      whenReady(AvroRecordFactory.build(request).failed)(_ shouldBe an[InvalidRequestException])
    }

    //validation
    it("returns invalid for payloads that do not conform to the schema") {
      val r = HydraRequest("1","""{"name":"test"}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val rec = AvroRecordFactory.build(r)
      whenReady(rec.failed)(_ shouldBe an[RequiredFieldMissingException])
    }

    it("validates good avro payloads") {
      val r = HydraRequest("1","""{"name":"test","rank":10}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      whenReady(AvroRecordFactory.build(r)) { rec =>
        val avroSchema = new Schema.Parser().parse(Source.fromResource("schema.avsc").mkString)
        val genericRecord = new GenericRecordBuilder(avroSchema).set("name", "test").set("rank", 10).build()
        val avroRecord = AvroRecord("test-topic", avroSchema, None, genericRecord)
        rec shouldBe avroRecord
      }
    }

  }
}
