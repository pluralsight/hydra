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
import akka.persistence.{AtLeastOnceDelivery, PersistentActor}
import com.typesafe.config.Config
import configs.syntax._
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.core.protocol._
import hydra.core.transport.RetryStrategy
import hydra.kafka.producer.{JsonRecord, KafkaRecord, KafkaRecordMetadata}
import hydra.kafka.transport.KafkaProducerProxy.{ProduceToKafka, ProduceToKafkaWithAck}
import hydra.kafka.transport.KafkaTransport.ProxiesInitialized
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.TimeoutException

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.language.existentials
import scala.util.{Failure, Success, Try}

/**
  * Created by alexsilva on 10/28/15.
  */
class KafkaTransport(producersConfig: Map[String, Config]) extends Actor with LoggingAdapter
  with ConfigSupport with Stash with PersistentActor with AtLeastOnceDelivery {

  override val persistenceId = "hydra-kafka-transport"

  val metricsEnabled = applicationConfig.get[Boolean]("producers.kafka.metrics.enabled").valueOrElse(false)

  lazy val metricsTopic = applicationConfig.get[String]("producers.kafka.metrics.topic").valueOrElse("HydraKafkaError")

  var producers: Map[String, ActorRef] = _

  implicit val ec = context.dispatcher

  override def receiveCommand: Receive = initializing

  def initializing: Receive = {
    case ProxiesInitialized =>
      context.become(transporting)
      unstashAll()
    case _ => stash()
  }

  private def transporting: Receive = {
    case p@Produce(r: KafkaRecord[_, _]) => transport(r, p)

    case p@ProduceWithAck(r: KafkaRecord[_, _], _, _) => transport(r, p)

    case p@RecordProduced(_: KafkaRecordMetadata) => confirm(p)

    case e: RecordNotProduced[_, _] => notProduced(e)
  }

  private def transport(kr: KafkaRecord[_, _], p: ProduceRecord[_, _]) = {
    getRecordProducer(kr) match {
      case Success(actorRef) =>
        kr.retryStrategy match {
          case RetryStrategy.Persist => persistAsync(p)(updateStore)
          case RetryStrategy.Ignore =>
            val msg = p match {
              case Produce(kr: KafkaRecord[_, _]) => ProduceToKafka(kr, 1)
              case ProduceWithAck(kr: KafkaRecord[_, _], ing, sup) => ProduceToKafkaWithAck(kr, ing, sup, 1)
            }
            actorRef ! msg
        }
      case Failure(ex) => sender ! RecordNotProduced(kr, ex)
    }
  }

  private def updateStore(evt: HydraMessage): Unit = evt match {
    case p@Produce(kr: KafkaRecord[_, _]) =>
      deliver(producers(kr.formatName).path)(deliveryId => ProduceToKafka(kr, deliveryId))

    case p@ProduceWithAck(kr: KafkaRecord[_, _], ingestor, supervisor) =>
      deliver(producers(kr.formatName).path)(deliveryId => ProduceToKafkaWithAck(kr, ingestor, supervisor, deliveryId))

    case RecordProduced(kmd) => confirmDelivery(kmd.deliveryId)
  }

  private def confirm(p: RecordProduced): Unit = {
    if (p.md.retryStrategy == RetryStrategy.Persist) persistAsync(p)(updateStore)
    recordStatistics(p.md.asInstanceOf[KafkaRecordMetadata])
  }

  private def notProduced(e: RecordNotProduced[_, _]) = {
    context.system.eventStream.publish(e)
  }

  private def recordStatistics(r: KafkaRecordMetadata) = {
    if (metricsEnabled) producers.get("json").foreach(_ ! ProduceToKafka(JsonRecord(metricsTopic, Some(r.topic), r), 0))
  }

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = -1, withinTimeRange = Duration.Inf) {
      case _: TimeoutException => Restart
      case _: ConnectException => Restart
      case _: KafkaException => Restart
      case _: Exception => Restart
    }

  override def receiveRecover: Receive = {
    case msg: HydraMessage => updateStore(msg)
  }

  override def preStart(): Unit = {
    initProducers()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    producers.values.foreach(_ ! PoisonPill)
    initProducers()
  }

  private def getRecordProducer(kr: KafkaRecord[_, _]): Try[ActorRef] = {
    Try(producers(kr.formatName))
      .recover { case _: NoSuchElementException =>
        throw new IllegalArgumentException(s"No producer for records with format ${kr.formatName}")
      }
  }

  private def initProducers() = {
    val prods = Future {
      producersConfig.map {
        case (format, config) =>
          format -> context.actorOf(KafkaProducerProxy.props(self, config), format)
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

  case object ProxiesInitialized

  def props(producersConfig: Map[String, Config]): Props = Props(classOf[KafkaTransport], producersConfig)
}

