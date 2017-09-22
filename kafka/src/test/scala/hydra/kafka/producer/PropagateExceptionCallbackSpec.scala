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
import hydra.core.protocol.{ProducerAck, RecordNotProduced}
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

  describe("When using json PropagateExceptionCallback") {
    it("sends the completion to the actor selection") {
      val record = StringRecord("test", "test")
      val e = new PropagateExceptionCallback(ActorSelection(probe.ref, Seq.empty), record, 112)
      val md = new RecordMetadata(new TopicPartition("test", 0), 0L, 1L, 1L, 1, 1, 1)
      e.onCompletion(md, null)
      probe.expectMsg(KafkaRecordMetadata(md, 112, record.deliveryStrategy))
    }

    it("sends the error to the actor selection") {
      val record = StringRecord("test", "test")
      val e = new PropagateExceptionCallback(ActorSelection(probe.ref, Seq.empty), record, 112)
      val md = new RecordMetadata(new TopicPartition("test", 0), 0L, 1L, 1L, 1, 1, 1)
      val err = new IllegalArgumentException("test")
      e.onCompletion(md, err)
      probe.expectMsg(RecordNotProduced(record, err))
    }

    it("sends the completion to the actor selection and acks the ingestor") {
      val record = StringRecord("test", "test")
      val e = new PropagateExceptionWithAckCallback(ActorSelection(probe.ref, Seq.empty), ingestor.ref,
        supervisor.ref, record, 112)
      val md = new RecordMetadata(new TopicPartition("test", 0), 0L, 1L, 1L, 1, 1, 1)
      e.onCompletion(md, null)
      probe.expectMsg(KafkaRecordMetadata(md, 112, record.deliveryStrategy))
      ingestor.expectMsg(ProducerAck(supervisor.ref, None))
    }

    it("sends the error to the actor selection and acks the ingestor") {
      val record = StringRecord("test", "test")
      val e = new PropagateExceptionWithAckCallback(ActorSelection(probe.ref, Seq.empty), ingestor.ref,
        supervisor.ref, record, 112)
      val md = new RecordMetadata(new TopicPartition("test", 0), 0L, 1L, 1L, 1, 1, 1)
      val err = new IllegalArgumentException("test")
      e.onCompletion(md, err)
      probe.expectMsg(RecordNotProduced(record, err))
      ingestor.expectMsg(ProducerAck(supervisor.ref, Some(err)))
    }
  }
}
