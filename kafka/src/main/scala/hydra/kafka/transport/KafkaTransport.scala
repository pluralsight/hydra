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
import hydra.core.transport.AckStrategy.{LocalAck, NoAck, TransportAck}
import hydra.core.transport.HydraRecordMetadata
import hydra.kafka.producer.{KafkaRecord, KafkaRecordMetadata}
import hydra.kafka.transport.KafkaProducerProxy.{ProduceToKafka, ProducerInitializationError}
import hydra.kafka.transport.KafkaTransport.{ProduceOnly, RecordProduceError}

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

  private def produce(p: Produce[_, _]) = {
    val kr = p.record.asInstanceOf[KafkaRecord[_, _]]
    producers.get(kr.formatName) match {
      case Some(producer) =>
        p.ack match {
          case NoAck =>
            sender ! RecordAccepted(p.supervisor)
            producer ! ProduceToKafka(0, kr, None)
          case LocalAck =>
            val ingestor = sender
            persistAsync(p) { msg =>
              val kafkaMsg = ProduceToKafka(msg.deliveryId, kr, None)
              updateState(kafkaMsg) //saves it to the journal and sends to Kafka
              ingestor ! RecordProduced(HydraRecordMetadata(p.deliveryId, System.currentTimeMillis), p.supervisor)
            }
          case TransportAck =>
            val ingestor = sender
            producer ! ProduceToKafka(0, kr, Some(context.actorSelection(ingestor.path), p.supervisor))
        }
      case None =>
        sender ! RecordNotProduced(0, kr,
          new IllegalArgumentException(s"A Kafka producer for records of type ${kr.formatName} could not found."),
          p.supervisor)
    }
  }


  override def receiveCommand: Receive = {
    case p@Produce(_: KafkaRecord[_, _], _, _, _) => produce(p)

    case ProduceOnly(kr: KafkaRecord[_, _]) =>
      producers.get(kr.formatName).foreach(_ ! ProduceToKafka(0, kr, None))

    case kmd: KafkaRecordMetadata =>
      confirm(kmd)
      metrics.saveMetrics(kmd)

    case e: RecordProduceError => context.system.eventStream.publish(e)

    case p: ProducerInitializationError => context.system.eventStream.publish(p)
  }

  private def updateState(evt: HydraMessage): Unit = evt match {
    case p@ProduceToKafka(_, kr, _) =>
      deliver(producers(kr.formatName).path)(deliveryId => p.copy(deliveryId = deliveryId))

    case kmd: KafkaRecordMetadata => confirm(kmd)
  }

  private def confirm(kmd: KafkaRecordMetadata): Unit = {
    persistAsync(kmd)(r => confirmDelivery(r.deliveryId))
  }


  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = -1, withinTimeRange = Duration.Inf) {
      case _: InvalidProducerSettingsException => Resume
      case _: Exception => Restart
    }

  override def receiveRecover: Receive = {
    case p@Produce(_: KafkaRecord[_, _], _, _, _) => produce(p)
    case msg: HydraMessage => updateState(msg)
  }

  override def preStart(): Unit = {
    producersConfig
      .foreach { case (f, c) => producers += f -> context.actorOf(KafkaProducerProxy.props(f, c), f) }
  }
}

object KafkaTransport {

  case class ProduceOnly(record: KafkaRecord[_, _])

  case class RecordProduceError(deliveryId: Long, record: KafkaRecord[_, _], err: Throwable)

  def props(producersConfig: Map[String, Config]): Props = Props(classOf[KafkaTransport], producersConfig)

}



