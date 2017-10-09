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
import akka.persistence.{AtLeastOnceDelivery, PersistentActor}
import com.typesafe.config.Config
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.core.protocol._
import hydra.core.transport.DeliveryStrategy
import hydra.kafka.producer.{KafkaRecord, KafkaRecordMetadata}
import hydra.kafka.transport.KafkaProducerProxy.{ProduceToKafka, ProduceToKafkaWithAck}

import scala.concurrent.duration.Duration
import scala.language.existentials

/**
  * Created by alexsilva on 10/28/15.
  */
class KafkaTransport(producersConfig: Map[String, Config]) extends Actor with LoggingAdapter
  with ConfigSupport with PersistentActor with AtLeastOnceDelivery {

  override val persistenceId = "hydra-kafka-transport"

  implicit val ec = context.dispatcher

  private[kafka] lazy val metrics = KafkaMetrics(applicationConfig)(context.system)

  private[kafka] val producers = new scala.collection.mutable.HashMap[String, ActorRef]()

  override def receiveCommand: Receive = {
    case p@Produce(_: KafkaRecord[_, _]) =>
      transport(p)

    case p@ProduceWithAck(_: KafkaRecord[_, _], _, _) =>
      transport(p)

    case p@RecordProduced(kmd: KafkaRecordMetadata) =>
      confirm(p)
      metrics.saveMetrics(kmd)

    case e: RecordNotProduced[_, _] => context.system.eventStream.publish(e)
  }

  private def transport(pr: ProduceRecord[_, _]) = {
    val kr = pr.record.asInstanceOf[KafkaRecord[_, _]]
    lookupProducer(kr) { producer =>
      kr.deliveryStrategy match {
        case DeliveryStrategy.AtLeastOnce => persistAsync(pr)(updateStore)
        case DeliveryStrategy.AtMostOnce => producer ! pr
      }
    }
  }

  private def lookupProducer(kr: KafkaRecord[_, _])(success: ActorRef => Unit) = {
    val format = kr.formatName
    producers.get(format) match {
      case Some(p) => success(p)
      case None => sender ! RecordNotProduced(kr,
        new IllegalArgumentException(s"A Kafka producer for records of type $format could not found."))
    }
  }

  private def updateStore(evt: HydraMessage): Unit = evt match {
    case Produce(kr: KafkaRecord[_, _]) =>
      deliver(producers(kr.formatName).path)(deliveryId => ProduceToKafka(kr, deliveryId))

    case ProduceWithAck(kr: KafkaRecord[_, _], ingestor, supervisor) =>
      deliver(producers(kr.formatName).path)(deliveryId => ProduceToKafkaWithAck(kr, ingestor, supervisor, deliveryId))

    case RecordProduced(kmd) => confirmDelivery(kmd.deliveryId)
  }

  private def confirm(p: RecordProduced): Unit = {
    if (p.md.retryStrategy == DeliveryStrategy.AtLeastOnce) {
      persistAsync(p)(r => confirmDelivery(r.md.deliveryId))
    }
  }


  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = -1, withinTimeRange = Duration.Inf) {
      case _: InvalidProducerSettingsException => Resume
      case _: Exception => Restart
    }

  override def receiveRecover: Receive = {
    case msg: HydraMessage => updateStore(msg)
  }

  override def preStart(): Unit = {
    producersConfig
      .foreach { case (f, c) => producers += f -> context.actorOf(KafkaProducerProxy.props(f, c), f) }
  }
}

object KafkaTransport {

  def props(producersConfig: Map[String, Config]): Props = Props(classOf[KafkaTransport], producersConfig)

}



