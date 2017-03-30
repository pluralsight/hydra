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

import java.net.ConnectException

import akka.actor.SupervisorStrategy._
import akka.actor._
import com.typesafe.config.Config
import configs.syntax._
import hydra.common.logging.LoggingAdapter
import hydra.core.notification.{HydraEvent, NotificationSupport}
import hydra.core.protocol.{Produce, ProduceWithAck, RecordNotProduced, RecordProduced}
import hydra.kafka.producer.{JsonRecord, KafkaRecord, KafkaRecordMetadata}
import hydra.kafka.transport.KafkaTransport.ProxiesInitialized
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.TimeoutException

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.language.existentials
import scala.util.{Failure, Success}

/**
  * Created by alexsilva on 10/28/15.
  */
class KafkaTransport(producersConfig: Map[String, Config]) extends Actor with LoggingAdapter
  with NotificationSupport with Stash {

  val metricsEnabled = applicationConfig.get[Boolean]("producers.kafka.metrics.enabled").valueOrElse(false)

  lazy val metricsTopic = applicationConfig.get[String]("producers.kafka.metrics.topic").valueOrElse("HydraKafkaError")

  var producers: Map[String, ActorRef] = _

  implicit val ec = context.dispatcher

  def receive = initializing

  def initializing: Receive = {
    case ProxiesInitialized =>
      context.become(receiving)
      unstashAll()
    case _ => stash()
  }

  def receiving: Receive = {
    case Produce(record: KafkaRecord[_, _]) =>
      producers(record.formatName) ! Produce(record)

    case ProduceWithAck(record: KafkaRecord[_, _], ingestor, supervisor) =>
      producers(record.formatName) ! ProduceWithAck(record, ingestor, supervisor)

    case RecordProduced(r: KafkaRecordMetadata) =>
      if (metricsEnabled) recordStatistics(r)

    case err: RecordNotProduced[_, _] => publishToEventStream(err)
  }

  private def publishToEventStream(error: RecordNotProduced[_, _]) = {
    context.system.eventStream.publish(error)
  }

  private def recordStatistics(r: KafkaRecordMetadata) = {
    producers.get("json").foreach(_ ! Produce(JsonRecord(metricsTopic, None, r)))
  }

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = -1, withinTimeRange = Duration.Inf) {
      case _: TimeoutException => Restart
      case _: ConnectException => Restart
      case _: KafkaException => Restart
      case _: Exception => Restart
    }

  override def preStart(): Unit = {
    initProducers()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    producers.values.foreach(_ ! PoisonPill)
    initProducers()
  }

  private def initProducers() = {
    val prods = Future {
      producersConfig.map {
        case (format, config) =>
          format -> context.actorOf(KafkaProducerProxy.props(self, config))
      }
    }

    prods onComplete {
      case Success(producers) =>
        this.producers = producers
        self ! ProxiesInitialized

      case Failure(ex) => throw ex //just throw
    }
  }
}

object KafkaTransport {

  case class MessageNotSentEvent(source: RecordNotProduced[Any, Any]) extends HydraEvent[RecordNotProduced[_, _]]

  case object ProxiesInitialized

  def props(producersConfig: Map[String, Config]): Props =
    Props(classOf[KafkaTransport], producersConfig)
}

