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

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import hydra.core.ingest.HydraRequest
import hydra.core.protocol.{InvalidRequest, MessageValidationResult, ValidRequest}

import scala.util.{Failure, Success, Try}

/**
  * Created by alexsilva on 1/11/17.
  */
object JsonRecordFactory extends KafkaRecordFactory[String, String] {

  val mapper = new ObjectMapper()

  override def build(request: HydraRequest):Try[KafkaRecord[String,String]] =
    Try(JsonRecord(getTopic(request), getKey(request), request.payload, request.deliveryStrategy))

  override def validate(request: HydraRequest): MessageValidationResult = {
    //TODO: Strict validation with a json schema
    parseJson(request.payload) match {
      case Success(x) => ValidRequest
      case Failure(e) => InvalidRequest(e)
    }
  }

  private def parseJson(json: String): Try[JsonNode] = Try(mapper.reader().readTree(json))

}

