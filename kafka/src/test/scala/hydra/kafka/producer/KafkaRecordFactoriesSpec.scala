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

import com.fasterxml.jackson.databind.ObjectMapper
import hydra.core.ingest.HydraRequest
import hydra.core.ingest.RequestParams.{HYDRA_KAFKA_TOPIC_PARAM, HYDRA_RECORD_FORMAT_PARAM, HYDRA_SCHEMA_PARAM}
import hydra.core.protocol.InvalidRequest
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecordBuilder
import org.scalatest.{FunSpecLike, Matchers}

import scala.io.Source

/**
  * Created by alexsilva on 1/11/17.
  */
class KafkaRecordFactoriesSpec extends Matchers with FunSpecLike {

  describe("When using KafkaRecordFactories") {
    it("handles avro") {
      val json = """{"name":"test", "rank":10}"""
      val request = HydraRequest(123, json)
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val record = KafkaRecordFactories.build(request)
      val avroSchema = new Schema.Parser().parse(Source.fromResource("schema.avsc").mkString)
      val genericRecord = new GenericRecordBuilder(avroSchema).set("name", "test").set("rank", 10).build()

      record.get shouldBe AvroRecord("test-topic", avroSchema, None, genericRecord)
    }

    it("handles json") {
      val json = """{"name":"test", "rank":10}"""
      val request = HydraRequest(123, json)
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_RECORD_FORMAT_PARAM -> "json")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val record = KafkaRecordFactories.build(request)
      record.get shouldBe JsonRecord("test-topic", None, new ObjectMapper().reader().readTree(json))
    }

    it("handles strings") {
      val json = """{"name":"test", "rank":10}"""
      val request = HydraRequest(123, json)
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_RECORD_FORMAT_PARAM -> "string")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val record = KafkaRecordFactories.build(request)
      record.get shouldBe StringRecord("test-topic", None, json)
    }

    it("validates json records") {
      val request = HydraRequest(123,"""{"name":"test"}""")
        .withMetadata(HYDRA_RECORD_FORMAT_PARAM -> "json")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val validation = KafkaRecordFactories.build(request)
      validation.get shouldBe a[JsonRecord]
    }

    it("validates string records") {
      val request = HydraRequest(123,"""{"name":"test"}""")
        .withMetadata(HYDRA_RECORD_FORMAT_PARAM -> "string")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val validation = KafkaRecordFactories.build(request)
      validation.get shouldBe a[StringRecord]
    }

    it("validates avro records") {
      val schema = Thread.currentThread().getContextClassLoader.getResource("schema.avsc").getFile
      val avroSchema = new Schema.Parser().parse(new File(schema))
      val json = """{"name":"test", "rank":10}"""
      val request = HydraRequest(123, json)
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val validation = AvroRecordFactory.build(request)
      validation.get shouldBe a[AvroRecord]
    }

    it("invalidates unknown formats") {
      val request = HydraRequest(123,"""{"name":"test"}""")
        .withMetadata(HYDRA_RECORD_FORMAT_PARAM -> "unknown-format")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val validation = KafkaRecordFactories.build(request)
      validation shouldBe InvalidRequest(_: IllegalArgumentException)
    }


    it("throws error with unknown formats") {
      val json = """{"name":"test", "rank":10}"""
      val request = HydraRequest(123, json)
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_RECORD_FORMAT_PARAM -> "unknown")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      intercept[IllegalArgumentException] {
        KafkaRecordFactories.build(request).get
      }
    }
  }
}