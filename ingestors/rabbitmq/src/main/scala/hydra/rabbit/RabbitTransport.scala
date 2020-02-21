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

import akka.actor.Props
import akka.pattern.ask
import akka.util.Timeout
import com.spingo.op_rabbit.Message.{Ack, ConfirmResponse, Fail, Nack}
import com.spingo.op_rabbit._
import com.typesafe.config.Config
import hydra.core.transport.Transport
import hydra.core.transport.Transport.Deliver

import scala.concurrent.duration._

class RabbitTransport(rabbitControlProps: Props) extends Transport {
  implicit val ec = context.dispatcher

  val rabbitControl = context.actorOf(rabbitControlProps)

  private def sendMessage(r: RabbitRecord) = {
    implicit val timeout = Timeout(3 seconds)
    val message = r.destinationType match {
      case RabbitRecord.DESTINATION_TYPE_EXCHANGE =>
        val pub = Publisher.exchange(r.destination)
        Message(r.payload.getBytes(), pub)
      case RabbitRecord.DESTINATION_TYPE_QUEUE =>
        val pub = Publisher.queue(r.destination)
        Message(r.payload.getBytes(), pub)
    }
    (rabbitControl ? message).mapTo[ConfirmResponse]
  }

  override def transport = {
    case Deliver(r: RabbitRecord, deliveryId, callback) =>
      sendMessage(r).foreach { result =>
        result match {
          case x: Ack =>
            callback.onCompletion(
              deliveryId,
              Some(
                RabbitRecordMetadata(
                  System.currentTimeMillis(),
                  x.id,
                  r.destination,
                  r.destinationType,
                  r.ackStrategy
                )
              ),
              None
            )
          case _: Nack =>
            callback.onCompletion(
              deliveryId,
              None,
              Some(
                RabbitProducerException(
                  "Rabbit returned Nack, record not produced"
                )
              )
            )
          case x: Fail =>
            callback.onCompletion(deliveryId, None, Some(x.exception))
        }
      }
  }
}

object RabbitTransport {
  // will be used in testing
  def props(p: Props): Props = Props(classOf[RabbitTransport], p)

  // $COVERAGE-OFF$
  def props(c: Config): Props =
    Props(
      classOf[RabbitTransport],
      Props(
        classOf[RabbitControl],
        Left(ConnectionParams.fromConfig(c.getConfig("op-rabbit.connection")))
      )
    )

  // $COVERAGE-ON$
}

case class RabbitProducerException(msg: String) extends Exception(msg)
