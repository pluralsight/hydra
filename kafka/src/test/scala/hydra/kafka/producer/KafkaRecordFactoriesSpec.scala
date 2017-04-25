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

import hydra.core.ingest.HydraRequest
import hydra.core.ingest.RequestParams.{HYDRA_KAFKA_TOPIC_PARAM, HYDRA_RECORD_FORMAT_PARAM, HYDRA_SCHEMA_PARAM}
import hydra.core.protocol.{InvalidRequest, ValidRequest}
import org.apache.avro.Schema
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 1/11/17.
  */
class KafkaRecordFactoriesSpec extends Matchers with FunSpecLike {

  describe("When using KafkaRecordFactories") {

    it("handles avro") {
      val json = """{"name":"test", "rank":10}"""
      val request = HydraRequest("test-topic", json)
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val record = KafkaRecordFactories.build(request)
      record shouldBe an[AvroRecord]
    }

    it("handles json") {
      val json = """{"name":"test", "rank":10}"""
      val request = HydraRequest("test-topic", json)
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_RECORD_FORMAT_PARAM -> "json")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val record = KafkaRecordFactories.build(request)
      record shouldBe an[JsonRecord]
    }

    it("handles strings") {
      val json = """{"name":"test", "rank":10}"""
      val request = HydraRequest("test-topic", json)
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_RECORD_FORMAT_PARAM -> "string")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val record = KafkaRecordFactories.build(request)
      record shouldBe an[StringRecord]
    }

    it("validates json records") {
      val request = HydraRequest("test-topic","""{"name":"test"}""")
        .withMetadata(HYDRA_RECORD_FORMAT_PARAM -> "json")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val validation = KafkaRecordFactories.validate(request)
      validation shouldBe ValidRequest
    }

    it("validates string records") {
      val request = HydraRequest("test-topic","""{"name":"test"}""")
        .withMetadata(HYDRA_RECORD_FORMAT_PARAM -> "string")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val validation = KafkaRecordFactories.validate(request)
      validation shouldBe ValidRequest
    }

    it("validates avro records") {
      val schema = Thread.currentThread().getContextClassLoader.getResource("schema.avsc").getFile
      val avroSchema = new Schema.Parser().parse(new File(schema))
      val json = """{"name":"test", "rank":10}"""
      val request = HydraRequest("test-topic", json)
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val validation = AvroRecordFactory.validate(request)
      validation shouldBe ValidRequest
    }

    it("invalidates unknown formats") {
      val request = HydraRequest("test-topic","""{"name":"test"}""")
        .withMetadata(HYDRA_RECORD_FORMAT_PARAM -> "unknown-format")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val validation = KafkaRecordFactories.validate(request)
      validation shouldBe InvalidRequest(_: IllegalArgumentException)
    }


    it("throws error with unknown formats") {
      val json = """{"name":"test", "rank":10}"""
      val request = HydraRequest("test-topic", json)
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
        .withMetadata(HYDRA_RECORD_FORMAT_PARAM -> "unknown")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      intercept[IllegalArgumentException] {
        KafkaRecordFactories.build(request)
      }
    }
  }
}