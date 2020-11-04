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

package hydra.kafka.ingestors

import akka.pattern.ask
import hydra.common.config.ConfigSupport._
import akka.util.Timeout
import hydra.core.ingest.RequestParams._
import hydra.core.ingest.{HydraRequest, Ingestor, RequestParams}
import hydra.core.protocol._
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.ingestors.KafkaTopicsActor.{
  GetTopicRequest,
  GetTopicResponse
}
import hydra.kafka.producer.{KafkaProducerSupport, KafkaRecordFactories}

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Sends JSON messages to a topic in Kafka.  In order for this handler to be activated.
  * a request param "Hydra-kafka-topic" must be present.
  *
  */
class KafkaIngestor extends Ingestor with KafkaProducerSupport {

  override val recordFactory = new KafkaRecordFactories(schemaRegistryActor)

  private val timeoutDuration = applicationConfig
    .getDurationOpt("kafka-ingestor-timeout")
    .getOrElse(2.seconds)

  private implicit val timeout = Timeout(timeoutDuration)

  ingest {
    case Publish(request) =>
      val hasTopic = request.metadataValue(HYDRA_KAFKA_TOPIC_PARAM).isDefined
      sender ! (if (hasTopic) Join else Ignore)

    case Ingest(record, ackStrategy) => transport(record, ackStrategy)
  }
}
