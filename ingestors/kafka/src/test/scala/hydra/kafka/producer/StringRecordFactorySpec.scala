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
import scala.concurrent.duration._

/**
  * Created by alexsilva on 1/11/17.
  */
class StringRecordFactorySpec
    extends Matchers
    with AnyFunSpecLike
    with ScalaFutures {

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(1000 millis),
    interval = scaled(100 millis)
  )
  describe("When using the StringRecordFactory") {

    it("handles valid strings") {
      val request = HydraRequest("123", """{"name":"test"}""").withMetadata(
        HYDRA_KAFKA_TOPIC_PARAM -> "test"
      )
      val rec = StringRecordFactory.build(request)
      whenReady(rec)(
        _ shouldBe StringRecord(
          "test",
          None,
          """{"name":"test"}""",
          AckStrategy.NoAck
        )
      )
    }

    it("builds") {
      val request = HydraRequest("123", """{"name":"test"}""")
        .withMetadata(HYDRA_RECORD_KEY_PARAM -> "{$.name}")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      whenReady(StringRecordFactory.build(request)) { msg =>
        msg.destination shouldBe "test-topic"
        msg.key shouldBe "test"
        msg.payload shouldBe """{"name":"test"}"""
      }
    }

    it("throws an error if no topic is in the request") {
      val request = HydraRequest("123", """{"name":test"}""")
      whenReady(StringRecordFactory.build(request).failed)(
        _ shouldBe an[MissingMetadataException]
      )
    }
  }
}
