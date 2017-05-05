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

import hydra.core.ingest.Ingestor
import hydra.core.ingest.RequestParams._
import hydra.core.protocol._
import hydra.core.transport.AckStrategy
import hydra.core.transport.AckStrategy.Explicit
import hydra.kafka.producer.{KafkaProducerSupport, KafkaRecordFactories}

import scala.util.{Failure, Success, Try}

/**
  * Sends JSON messages to a topic in Kafka.  In order for this handler to be activated.
  * a request param "Hydra-Transport" must be set to "Kafka".
  *
  */
class KafkaIngestor extends Ingestor with KafkaProducerSupport {

  ingest {
    case Publish(request) =>
      val hasTopic = request.metadataValue(HYDRA_KAFKA_TOPIC_PARAM).isDefined
      sender ! (if (hasTopic) Join else Ignore)

    case Validate(request) =>
      val validation = request.metadataValue(HYDRA_KAFKA_TOPIC_PARAM) match {
        case None => InvalidRequest(new IllegalArgumentException("A topic name must be supplied."))
        case Some(_) => KafkaRecordFactories.validate(request)
      }

      sender ! validation

    case Ingest(request) =>
      Try(KafkaRecordFactories.build(request)) match {
        case Success(record) =>
          request.ackStrategy match {
            case AckStrategy.None =>
              kafkaProducer ! Produce(record)
              sender ! IngestorCompleted

            case Explicit =>
              kafkaProducer ! ProduceWithAck(record, self, sender)
          }
        case Failure(ex) =>
          sender ! IngestorError(ex)
      }
  }
}
