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

package hydra.kafka.config

import hydra.common.config.ConfigSupport

/**
  * Created by alexsilva on 11/30/15.
  */
object KafkaConfigSupport extends ConfigSupport {

  val kafkaConfig = applicationConfig.withOnlyPath("kafka")

  val zkString = kafkaConfig.getString("kafka.consumer.zookeeper.connect")

  val bootstrapServers =
    kafkaConfig.getString("kafka.producer.bootstrap.servers")

  val schemaRegistryUrl: String =
    applicationConfig.getString("schema.registry.url")
}
