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
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * Created by alexsilva on 1/11/17.
  */
class DeleteTombstoneRecordFactorySpec
    extends Matchers
    with AnyFunSpecLike
    with ScalaFutures {

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(1000 millis),
    interval = scaled(100 millis)
  )
  describe("When using the DeleteTombstoneRecordFactory") {

    it("errors without keys") {
      val request = HydraRequest("123", payload = null)
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test")
      val rec = DeleteTombstoneRecordFactory.build(request)
      whenReady(rec.failed)(_ shouldBe an[IllegalArgumentException])
    }

    it("builds a delete record") {
      val request = HydraRequest("123", null)
        .withMetadata(HYDRA_RECORD_KEY_PARAM -> "key")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      whenReady(DeleteTombstoneRecordFactory.build(request)) { rec =>
        rec.destination shouldBe "test-topic"
        rec.key shouldBe "key"
        rec.formatName shouldBe "string"
        Option(rec.payload).isDefined shouldBe false
      }
    }
  }
}
