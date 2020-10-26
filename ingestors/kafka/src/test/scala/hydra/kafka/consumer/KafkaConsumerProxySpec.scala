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

package hydra.kafka.consumer

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import hydra.kafka.consumer.KafkaConsumerProxy._
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.TopicPartition
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.duration._

/**
  * Created by alexsilva on 9/7/16.
  */
class KafkaConsumerProxySpec
    extends TestKit(ActorSystem("test"))
    with Matchers
    with AnyFunSpecLike
    with BeforeAndAfterAll
    with ImplicitSender {

  implicit val config =
    EmbeddedKafkaConfig(kafkaPort = 8012, zooKeeperPort = 3111)

  override def beforeAll() = {
    super.beforeAll()
    EmbeddedKafka.start()
    EmbeddedKafka.createCustomTopic("test-consumer1")
    EmbeddedKafka.createCustomTopic("test-consumer2")
  }

  override def afterAll() = {
    super.afterAll()
    EmbeddedKafka.stop()
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  lazy val kafkaProxy = system.actorOf(Props[KafkaConsumerProxy])

  describe("When using KafkaConsumerProxy") {
    it("gets latest offsets for a topic") {
      kafkaProxy ! GetLatestOffsets("test-consumer1")
      expectMsg(
        10.seconds,
        LatestOffsetsResponse(
          "test-consumer1",
          Map(new TopicPartition("test-consumer1", 0) -> 0L)
        )
      )
    }

    it("lists topics") {
      kafkaProxy ! ListTopics
      expectMsgPF(10.seconds) {
        case ListTopicsResponse(topics) =>
          topics.keys should contain allOf ("test-consumer1", "test-consumer2")
      }
    }

    it("gets partition info") {
      kafkaProxy ! GetPartitionInfo("test-consumer2")
      expectMsgPF(10.seconds) {
        case PartitionInfoResponse(topic, response) =>
          topic shouldBe "test-consumer2"
          response.map(p => p.partition()) shouldBe Seq(0)
      }
    }

    it("handles errors") {
      kafkaProxy ! GetPartitionInfo("test-consumer-unknown")
      expectMsgPF(10.seconds) {
        case PartitionInfoResponse(topic, response) =>
          response(0).leader().idString shouldBe "0"
          topic should startWith("test-consumer-unknown")
      }
    }
  }
}
