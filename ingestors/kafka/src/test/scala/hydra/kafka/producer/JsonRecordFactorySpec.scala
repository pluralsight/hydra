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

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.ObjectMapper
import hydra.core.ingest.HydraRequest
import hydra.core.ingest.RequestParams.{
  HYDRA_KAFKA_TOPIC_PARAM,
  HYDRA_RECORD_KEY_PARAM
}
import hydra.core.protocol.MissingMetadataException
import hydra.core.transport.AckStrategy
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by alexsilva on 1/11/17.
  */
class JsonRecordFactorySpec
    extends Matchers
    with AnyFunSpecLike
    with ScalaFutures {

  describe("When using the JsonRecordFactory") {

    it("errors with no topic on the request") {
      val request = HydraRequest("123", """{"name":test"}""")
      val rec = JsonRecordFactory.build(request)
      whenReady(rec.failed)(_ shouldBe an[MissingMetadataException])
    }

    it("handles invalid json") {
      val request = HydraRequest("123", """{"name":test"}""")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val rec = JsonRecordFactory.build(request)
      whenReady(rec.failed)(_ shouldBe a[JsonParseException])
    }

    it("handles valid json") {
      val request = HydraRequest("123", """{"name":"test"}""")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val rec = JsonRecordFactory.build(request)
      val node = new ObjectMapper().reader().readTree("""{"name":"test"}""")
      whenReady(rec)(
        _ shouldBe JsonRecord("test-topic", None, node, AckStrategy.NoAck)
      )
    }

    it("builds") {
      val request = HydraRequest("123", """{"name":"test"}""")
        .withMetadata(HYDRA_RECORD_KEY_PARAM -> "{$.name}")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      whenReady(JsonRecordFactory.build(request)) { msg =>
        msg.destination shouldBe "test-topic"
        msg.key shouldBe "test"
        msg.payload shouldBe new ObjectMapper()
          .reader()
          .readTree("""{"name":"test"}""")
      }
    }

    it("throws an error if no topic is in the request") {
      val request = HydraRequest("123", """{"name":"test"}""")
      whenReady(JsonRecordFactory.build(request).failed)(
        _ shouldBe a[MissingMetadataException]
      )
    }
  }
}
