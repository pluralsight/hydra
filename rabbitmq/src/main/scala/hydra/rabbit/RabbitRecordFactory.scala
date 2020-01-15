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

import hydra.common.config.ConfigSupport
import hydra.core.ingest.HydraRequest
import hydra.core.transport.{AckStrategy, HydraRecord, RecordFactory, RecordMetadata}
import hydra.rabbit.RabbitRecord.{DESTINATION_TYPE_EXCHANGE, DESTINATION_TYPE_QUEUE, HYDRA_RABBIT_EXCHANGE, HYDRA_RABBIT_QUEUE}
import org.apache.commons.lang3.StringUtils

import scala.concurrent.{ExecutionContext, Future}

object RabbitRecordFactory extends RecordFactory[String, String] with ConfigSupport {
  override def build(request: HydraRequest)(implicit ec: ExecutionContext): Future[RabbitRecord] = {
    val props = Seq(request.metadataValue(HYDRA_RABBIT_EXCHANGE),
      request.metadataValue(HYDRA_RABBIT_QUEUE)).flatten
    Future {
      require(props.length == 1, "A single parameter for exchange or queue is required")
      val destination = request.metadataValue(HYDRA_RABBIT_EXCHANGE) match {
        case Some(exchange) => (exchange, DESTINATION_TYPE_EXCHANGE)
        case _ => (request.metadataValue(HYDRA_RABBIT_QUEUE).get, DESTINATION_TYPE_QUEUE)
      }
      RabbitRecord(destination._1, destination._2, request.payload, request.ackStrategy)
    }
  }
}

case class RabbitRecord(destination: String,
                        destinationType: String,
                        payload: String,
                        ackStrategy: AckStrategy)
  extends HydraRecord[String, String] {

  override val key: String = StringUtils.EMPTY
}

object RabbitRecord {

  val HYDRA_RABBIT_EXCHANGE = "hydra-rabbit-exchange"

  val HYDRA_RABBIT_QUEUE = "hydra-rabbit-queue"

  val DESTINATION_TYPE_EXCHANGE = "exchange"

  val DESTINATION_TYPE_QUEUE = "queue"

}

case class RabbitRecordMetadata(timestamp: Long, id: Long, destination: String,
                                destinationType: String, ackStrategy: AckStrategy) extends RecordMetadata