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

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Stash}
import akka.testkit.{ImplicitSender, TestActor, TestActorRef, TestKit, TestProbe}
import com.newmotion.akka.rabbitmq.ChannelCreated
import com.rabbitmq.client.AMQP.Confirm
import com.spingo.op_rabbit.{ConnectionParams, Message}
import com.spingo.op_rabbit.Message.{ConfirmResponse, _}
import hydra.common.config.ConfigSupport
import hydra.core.transport.{RecordMetadata, TransportCallback}
import hydra.core.transport.TransportSupervisor.Deliver
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

class RabbitTransportSpec extends TestKit(ActorSystem("hydra-test")) with Matchers with FunSpecLike
  with ImplicitSender with BeforeAndAfterAll with ConfigSupport {

  val probe = TestProbe()

//  object RabbitControlMock{}

  val testRabbitControl = Props[RabbitControlMock] //Props.empty //Props[RabbitControlMock]
  val rabbitTransport = TestActorRef[RabbitTransport](RabbitTransport.props(testRabbitControl), "rabbit_transport")

  //val c =  rootConfig.getConfig("op-rabbit") //applicationConfig.getConfig("op-rabbit") //.get("op-rabbit")
  //val rabbitTransport = TestActorRef[RabbitTransport](RabbitTransport.props(c), "rabbit_transport")

  override def afterAll = TestKit.shutdownActorSystem(system, verifySystemShutdown = true)

  describe("When using the RabbitTransport") {
    it("sends valid exchange messages and receive ack") {
      val rec = RabbitRecord("test.exchange", RabbitRecord.DESTINATION_TYPE_EXCHANGE, "exchange-ack")
      val ack: TransportCallback = (d: Long, md: Option[RecordMetadata], err: Option[Throwable]) => probe.ref ! md.get
      val m = Deliver(rec,1,ack)
      rabbitTransport ! m
      probe.expectMsgPF() {
        case md:RabbitRecordMetadata =>
          md.id shouldBe 11
          md.destination shouldBe "test.exchange"
          md.destinationType shouldBe RabbitRecord.DESTINATION_TYPE_EXCHANGE
      }
    }

    it("sends valid queue messages and receive ack") {
      val rec = RabbitRecord("test.queue", RabbitRecord.DESTINATION_TYPE_QUEUE, "queue-ack")
      val ack: TransportCallback = (d: Long, md: Option[RecordMetadata], err: Option[Throwable]) => probe.ref ! md.get
      val m = Deliver(rec,1,ack)
      rabbitTransport ! m
      probe.expectMsgPF() {
        case md:RabbitRecordMetadata =>
          md.id shouldBe 12
          md.destination shouldBe "test.queue"
          md.destinationType shouldBe RabbitRecord.DESTINATION_TYPE_QUEUE
      }
    }

    it("receives error on Fail condition") {
      val rec = RabbitRecord("test.exchange", RabbitRecord.DESTINATION_TYPE_EXCHANGE, "exchange-Fail")
      val err: TransportCallback = (d: Long, md: Option[RecordMetadata], err: Option[Throwable]) => probe.ref ! err.get
      val m = Deliver(rec,1,err)
      rabbitTransport ! m
      probe.expectMsgType[IllegalArgumentException]
    }

    it("receives error on Nack condition") {
      val rec = RabbitRecord("test.exchange", RabbitRecord.DESTINATION_TYPE_EXCHANGE, "exchange-Nack")
      val err: TransportCallback = (d: Long, md: Option[RecordMetadata], err: Option[Throwable]) => probe.ref ! err.get
      val m = Deliver(rec,1,err)
      rabbitTransport ! m
      probe.expectMsgType[RabbitProducerException]
    }

  }
}

class RabbitControlMock extends Actor with ActorLogging {
  override def receive = {
    case m: Message if (new String(m.data) =="exchange-ack") =>
      sender ! Ack(11)
    case m: Message if (new String(m.data) =="queue-ack") =>
      sender ! Ack(12)
    case m: Message if (new String(m.data) =="exchange-Fail") =>
      sender ! Fail(13, new IllegalArgumentException)
    case m: Message if (new String(m.data) =="exchange-Nack") =>
      sender ! Nack(14)
  }
}



