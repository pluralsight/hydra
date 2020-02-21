/*
 * Copyright (C) 2017 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hydra.rabbit

import hydra.core.ingest.HydraRequest
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpecLike, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global

class RabbitRecordFactorySpec
    extends Matchers
    with FunSpecLike
    with ScalaFutures {
  describe("The Rabbit record factory") {
    it("throws an error if no exchange or queue metadata provided") {
      val request = HydraRequest("123", "{'name': 'test'}")
      whenReady(RabbitRecordFactory.build(request).failed)(
        _ shouldBe an[IllegalArgumentException]
      )
    }

    it("throws an error if both exchange and queue metadata provided") {
      val request = HydraRequest("123", "{'name': 'test'}").withMetadata(
        RabbitRecord.HYDRA_RABBIT_EXCHANGE -> "test.exchange",
        RabbitRecord.HYDRA_RABBIT_QUEUE -> "test.queue"
      )
      whenReady(RabbitRecordFactory.build(request).failed)(
        _ shouldBe an[IllegalArgumentException]
      )
    }

    it("builds a record with the exchange") {
      val request = HydraRequest("123", "{'name': 'test'}").withMetadata(
        RabbitRecord.HYDRA_RABBIT_EXCHANGE -> "test.exchange"
      )
      whenReady(RabbitRecordFactory.build(request)) { rec =>
        rec.destination shouldBe "test.exchange"
        rec.destinationType shouldBe RabbitRecord.DESTINATION_TYPE_EXCHANGE
        rec.payload shouldBe "{'name': 'test'}"
      }
    }

    it("builds a record with the queue") {
      val request = HydraRequest("123", "{'name': 'test'}").withMetadata(
        RabbitRecord.HYDRA_RABBIT_QUEUE -> "test.queue"
      )
      whenReady(RabbitRecordFactory.build(request)) { rec =>
        rec.destination shouldBe "test.queue"
        rec.destinationType shouldBe RabbitRecord.DESTINATION_TYPE_QUEUE
        rec.payload shouldBe "{'name': 'test'}"
      }
    }
  }
}
