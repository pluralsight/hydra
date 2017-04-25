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
import hydra.core.avro.JsonToAvroConversionExceptionWithMetadata
import hydra.core.ingest.RequestParams._
import hydra.core.ingest.{HydraRequest, InvalidRequestException}
import hydra.core.protocol.InvalidRequest
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 1/11/17.
  */
class AvroRecordFactorySpec extends Matchers with FunSpecLike {

  val schema = Thread.currentThread().getContextClassLoader.getResource("schema.avsc").getFile

  describe("When performing validation") {
    it("handles Avro default value errors") {
      val request = HydraRequest("test-topic","""{"name":"test"}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val validation = AvroRecordFactory.validate(request)
      val ex = validation.asInstanceOf[InvalidRequest].error.asInstanceOf[JsonToAvroConversionExceptionWithMetadata]
      ex.cause shouldBe an[RequiredFieldMissingException]
    }

    it("handles fields not defined in the schema") {
      val request = HydraRequest("test-topic","""{"name":"test","rank":1,"new-field":"new"}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
        .withMetadata(HYDRA_VALIDATION_STRATEGY -> "strict")
      val validation = KafkaRecordFactories.validate(request)
      val ex = validation.asInstanceOf[InvalidRequest].error.asInstanceOf[JsonToAvroConversionExceptionWithMetadata]
      ex.cause shouldBe an[UndefinedFieldsException]

    }

    it("handles Avro datatype errors") {
      val request = HydraRequest("test-topic","""{"name":"test", "rank":"booyah"}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val validation = AvroRecordFactory.validate(request)
      val ex = validation.asInstanceOf[InvalidRequest].error.asInstanceOf[JsonToAvroConversionExceptionWithMetadata]
      ex.cause shouldBe an[InvalidDataTypeException]
    }

    it("builds keyless messages") {
      val avroSchema = new Schema.Parser().parse(new File(schema))
      val json = """{"name":"test", "rank":10}"""
      val request = HydraRequest("test-topic", json)
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val msg = AvroRecordFactory.build(request)
      msg.destination shouldBe "test-topic"
      msg.key shouldBe None
      msg.schema shouldBe avroSchema
      msg.json shouldBe json
      msg.payload shouldBe new JsonConverter[GenericRecord](avroSchema).convert(json)
    }

    it("builds keyed messages") {
      val avroSchema = new Schema.Parser().parse(new File(schema))
      val json = """{"name":"test", "rank":10}"""
      val request = HydraRequest("test-topic", json)
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_RECORD_KEY_PARAM -> "{$.name}")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val msg = AvroRecordFactory.build(request)
      msg.destination shouldBe "test-topic"
      msg.schema shouldBe avroSchema
      msg.json shouldBe json
      msg.key shouldBe Some("test")
      msg.payload shouldBe new JsonConverter[GenericRecord](avroSchema).convert(json)
    }

    it("has the right subject when a schema is specified") {
      val request = HydraRequest("test-topic","""{"name":"test", "rank":10}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      AvroRecordFactory.getSubject(request) shouldBe "classpath:schema.avsc"
    }

    it("defaults to target as the subject") {
      val request = HydraRequest("test-topic","""{"name":"test", "rank":10}""")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      AvroRecordFactory.getSubject(request) shouldBe "test-topic"
    }

    it("throws an error if no topic is in the request") {
      val request = HydraRequest("test-topic","""{"name":test"}""")
      intercept[InvalidRequestException] {
        AvroRecordFactory.build(request)
      }
    }

  }
}
