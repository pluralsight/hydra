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

package hydra.kafka.transport

import java.util.concurrent.atomic.AtomicLong

import akka.actor.SupervisorStrategy._
import akka.actor._
import akka.kafka.ProducerSettings
import com.typesafe.config.Config
import hydra.core.monitor.HydraMetrics
import hydra.core.transport.Transport
import hydra.core.transport.Transport.Deliver
import hydra.kafka.producer.{KafkaRecord, KafkaRecordMetadata}
import hydra.kafka.transport.KafkaProducerProxy.{
  ProduceToKafka,
  ProducerInitializationError
}
import hydra.kafka.transport.KafkaTransport.{RecordProduceError, ReportMetrics}
import hydra.kafka.util.KafkaUtils

import scala.concurrent.duration._
import scala.language.existentials

/**
  * Created by alexsilva on 10/28/15.
  */
class KafkaTransport(producerSettings: Map[String, ProducerSettings[Any, Any]])
    extends Transport
    with Timers {

  private type KR = KafkaRecord[_, _]

  private[kafka] lazy val metrics =
    KafkaMetrics(applicationConfig)(context.system)

  private[kafka] val msgCounter = new AtomicLong()

  timers.startTimerAtFixedRate("kamon", ReportMetrics, 1.minute)

  override def transport: Receive = {
    case Deliver(kr: KafkaRecord[_, _], deliveryId, ack) =>
      withProducer(kr.formatName)(_ ! ProduceToKafka(deliveryId, kr, ack))(e =>
        ack.onCompletion(deliveryId, None, e)
      )

    case kmd: KafkaRecordMetadata =>
      msgCounter.incrementAndGet()
      metrics.saveMetrics(kmd)

    case e: RecordProduceError =>
      context.system.eventStream.publish(e)

    case p: ProducerInitializationError => context.system.eventStream.publish(p)
  }

  private def withProducer(
      id: String
  )(success: ActorRef => Unit)(fail: Option[Throwable] => Unit) = {
    context.child(id) match {
      case Some(producer) => success(producer)
      case None =>
        fail(
          Some(
            new IllegalArgumentException(s"No Kafka producer named $id found.")
          )
        )
    }
  }

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = -1, withinTimeRange = Duration.Inf) {
      case _: InvalidProducerSettingsException => Resume
      case _: Exception                        => Restart
    }

  override def preStart(): Unit = {
    producerSettings.foreach {
      case (id, s) =>
        context.actorOf(KafkaProducerProxy.props(id, s), id)
    }
  }

  override def postStop(): Unit = metrics.close()
}

object KafkaTransport {

  private[kafka] val histogramMetricName =
    "hydra_ingest_records_published_total_minutes_bucket"

  private[kafka] val counterMetricName = "hydra_ingest_records_published_total"

  case class RecordProduceError(
      deliveryId: Long,
      record: KafkaRecord[_, _],
      error: Throwable
  )

  case object ReportMetrics

  /**
    * Method to comply with TransportRegistrar that looks for a method in the companion object called props
    * that takes a config param.
    *
    * @param cfg - We are not using this (this is the rootConfig)
    * @return
    */
  def props(cfg: Config): Props =
    Props(classOf[KafkaTransport], KafkaUtils.producerSettings(cfg))

}
