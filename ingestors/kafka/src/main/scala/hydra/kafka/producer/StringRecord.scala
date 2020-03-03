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

import hydra.core.transport.AckStrategy
import org.apache.commons.lang3.StringUtils

/**
  * Created by alexsilva on 11/30/15.
  */
case class StringRecord(
    destination: String,
    key: String,
    payload: String,
    ackStrategy: AckStrategy
) extends KafkaRecord[String, String]

object StringRecord {

  def apply(
      topic: String,
      key: Option[String],
      payload: String,
      ackStrategy: AckStrategy
  ): StringRecord =
    new StringRecord(topic, key.orNull, payload, ackStrategy)
}
