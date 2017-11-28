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

import akka.actor.SupervisorStrategy._
import akka.actor._
import com.typesafe.config.Config
import hydra.core.transport.Transport
import hydra.core.transport.TransportSupervisor.Deliver
import hydra.kafka.producer.{KafkaRecord, KafkaRecordMetadata}
import hydra.kafka.transport.KafkaProducerProxy.{ProduceToKafka, ProducerInitializationError}
import hydra.kafka.transport.KafkaTransport.RecordProduceError

import scala.concurrent.duration.Duration
import scala.language.existentials

/**
  * Created by alexsilva on 10/28/15.
  */
class KafkaTransport(producersConfig: Map[String, Config]) extends Transport {

  private type KR = KafkaRecord[_, _]

  private[kafka] lazy val metrics = KafkaMetrics(applicationConfig)(context.system)

  private[kafka] val producers = new scala.collection.mutable.HashMap[String, ActorRef]()

  override def receive: Receive = {
    case Deliver(kr: KafkaRecord[_, _], deliveryId, ack) =>
      withProducer(kr.formatName)(_ ! ProduceToKafka(deliveryId, kr, ack))(e => ack.onCompletion(deliveryId, None, e))

    case kmd: KafkaRecordMetadata => metrics.saveMetrics(kmd)

    case e: RecordProduceError => context.system.eventStream.publish(e)

    case p: ProducerInitializationError => context.system.eventStream.publish(p)
  }


  private def withProducer(format: String)(success: (ActorRef) => Unit)(fail: (Option[Throwable]) => Unit) = {
    producers.get(format) match {
      case Some(producer) => success(producer)
      case None => fail(Some(new IllegalArgumentException(s"No Kafka producer for $format records found.")))
    }
  }

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = -1, withinTimeRange = Duration.Inf) {
      case _: InvalidProducerSettingsException => Resume
      case _: Exception => Restart
    }


  override def preStart(): Unit = {
    producersConfig
      .foreach { case (f, c) => producers += f -> context.actorOf(KafkaProducerProxy.props(f, c), f) }
  }
}

object KafkaTransport {

  case class RecordProduceError(deliveryId: Long, record: KafkaRecord[_, _], error: Throwable)

  def props(producersConfig: Map[String, Config]): Props = Props(classOf[KafkaTransport], producersConfig)

}





