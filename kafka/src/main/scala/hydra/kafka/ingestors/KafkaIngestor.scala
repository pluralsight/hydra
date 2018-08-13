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

import hydra.core.ingest.RequestParams._
import hydra.core.ingest.{HydraRequest, Ingestor}
import hydra.core.monitor.HydraMetrics
import hydra.core.protocol._
import hydra.kafka.producer.{KafkaProducerSupport, KafkaRecordFactories}

import scala.util.Try

/**
  * Sends JSON messages to a topic in Kafka.  In order for this handler to be activated.
  * a request param "Hydra-kafka-topic" must be present.
  *
  */
class KafkaIngestor extends Ingestor with KafkaProducerSupport {
  import KafkaIngestor._

  override val recordFactory = new KafkaRecordFactories(schemaRegistryActor)

  ingest {
    case Publish(request) =>
      val hasTopic = request.metadataValue(HYDRA_KAFKA_TOPIC_PARAM).isDefined
      sender ! (if (hasTopic) Join else Ignore)

    case Ingest(record, ackStrategy) => transport(record, ackStrategy)
  }

  override def validateRequest(request: HydraRequest): Try[HydraRequest] = {
    val topic = request.metadataValue(HYDRA_KAFKA_TOPIC_PARAM).getOrElse("topicMissing")

    HydraMetrics.incrementGauge(
      lookupKey = ReconciliationGaugeName + s"_$topic",
      metricName = ReconciliationMetricName,
      tags = Seq("ingestType" -> "kafka", "topic" -> topic)
    )

    super.validateRequest(request)
  }
}

object KafkaIngestor {
  val ReconciliationGaugeName = "hydra_ingest_kafka_reconciliation"
  val ReconciliationMetricName = "ingest_kafka_reconciliation"
}
