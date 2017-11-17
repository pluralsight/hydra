/*
 * Copyright (C) 2017 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hydra.kafka.producer

import akka.actor.{ActorSelection, ActorSystem}
import akka.testkit.{TestKit, TestProbe}
import hydra.core.protocol.{RecordNotProduced, RecordProduced}
import hydra.core.transport.HydraRecord
import hydra.core.transport.Transport.AckCallback
import hydra.kafka.transport.KafkaTransport.RecordProduceError
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * Created by alexsilva on 1/11/17.
  */
class PropagateExceptionCallbackSpec extends TestKit(ActorSystem("hydra")) with Matchers with FunSpecLike
  with BeforeAndAfterAll {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  val probe = TestProbe()
  val ingestor = TestProbe()
  val supervisor = TestProbe()

  private def callback(record: HydraRecord[_, _]): AckCallback =
    (md, err) => ingestor.ref ! (md.map(RecordProduced(_, supervisor.ref))
      .getOrElse(RecordNotProduced(record, err.get, supervisor.ref)))

  describe("When using json PropagateExceptionCallback") {
    it("sends the completion to the actor selection") {
      val record = StringRecord("test", "test")
      val e = new PropagateExceptionWithAckCallback(112, record, ActorSelection(probe.ref, Seq.empty), callback(record))
      val md = new RecordMetadata(new TopicPartition("test", 0), 0L, 1L, 1L, 1L: java.lang.Long, 1, 1)
      e.onCompletion(md, null)
      probe.expectMsg(KafkaRecordMetadata(md, 112))
    }

    it("sends the error to the actor selection") {
      val record = StringRecord("test", "test")
      val e = new PropagateExceptionWithAckCallback(112, record,
        ActorSelection(probe.ref, Seq.empty), callback(record))
      val md = new RecordMetadata(new TopicPartition("test", 0), 0L, 1L, 1L, 1L: java.lang.Long, 1, 1)
      val err = new IllegalArgumentException("test")
      e.onCompletion(md, err)
      probe.expectMsg(RecordProduceError(112, record, err))
    }

    it("sends the completion to the actor selection and acks the ingestor") {
      val record = StringRecord("test", "test")
      val e = new PropagateExceptionWithAckCallback(112, record,
        ActorSelection(probe.ref, Seq.empty), callback(record))
      val md = new RecordMetadata(new TopicPartition("test", 0), 0L, 1L, 1L, 1L: java.lang.Long, 1, 1)
      e.onCompletion(md, null)
      probe.expectMsg(KafkaRecordMetadata(md, 112))
      ingestor.expectMsg(RecordProduced(KafkaRecordMetadata(md, 112), supervisor.ref))
    }

    it("sends the error to the actor selection and acks the ingestor") {
      val record = StringRecord("test", "test")
      val e = new PropagateExceptionWithAckCallback(112, record,
        ActorSelection(probe.ref, Seq.empty), callback(record))
      val md = new RecordMetadata(new TopicPartition("test", 0), 0L, 1L, 1L, 1L: java.lang.Long, 1, 1)
      val err = new IllegalArgumentException("test")
      e.onCompletion(md, err)
      probe.expectMsgPF() {
        case RecordProduceError(112, r, e) =>
          r shouldBe record
          e.getMessage shouldBe "test"
          e shouldBe a[IllegalArgumentException]
      }
      ingestor.expectMsgPF() {
        case RecordNotProduced(r, ex, s) =>
          r shouldBe record
          ex.getMessage shouldBe err.getMessage
          s shouldBe supervisor.ref
      }
    }
  }
}
