/*
 * Copyright (C) 2016 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package hydra.kafka.producer

import com.fasterxml.jackson.databind.ObjectMapper
import hydra.core.transport.DeliveryStrategy
import hydra.core.transport.DeliveryStrategy.BestEffort

/**
  * Created by alexsilva on 11/30/15.
  *
  * A Jackson backed JSON record implementation, where the key is a string object and the payload is a String
  * converted using Jackson.
  */
case class JsonRecord(destination: String, key: Option[String], payload: String,
                      deliveryStrategy: DeliveryStrategy = BestEffort) extends KafkaRecord[String, String]

object JsonRecord {
  val mapper = new ObjectMapper()

  def apply(topic: String, key: Option[String], obj: Any): JsonRecord = {
    val payload: String = mapper.writeValueAsString(obj)
    new JsonRecord(topic, key, payload)
  }
}
