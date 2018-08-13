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

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestActors.ForwardActor
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import hydra.core.ingest.HydraRequest
import hydra.core.protocol._
import hydra.core.transport.{AckStrategy, HydraRecord}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.duration._

class RabbitIngestorSpec extends TestKit(ActorSystem("rabbit-ingestor-spec")) with Matchers with FunSpecLike with ImplicitSender
  with BeforeAndAfterAll {

  val ingestor = system.actorOf(Props[RabbitIngestor])

  val probe = TestProbe()

  val rabbitTransport = system.actorOf(Props(new ForwardActor(probe.ref)), "rabbit_transport")

  override def afterAll = TestKit.shutdownActorSystem(system, verifySystemShutdown = true)

  describe("When using the rabbit ingestor") {
    it("Joins if exchange provided") {
      val request = HydraRequest("123", "{'name': 'test'}", None,
        Map(RabbitRecord.HYDRA_RABBIT_EXCHANGE -> "test.exchange"))
      ingestor ! Publish(request)
      expectMsg(10.seconds, Join)
    }

    it("Joins if queue provided") {
      val request = HydraRequest("123", "{'name': 'test'}", None,
        Map(RabbitRecord.HYDRA_RABBIT_QUEUE -> "test.queue"))
      ingestor ! Publish(request)
      expectMsg(10.seconds, Join)
    }

    it("Ignores") {
      val request = HydraRequest("123", "test string")
      ingestor ! Publish(request)
      expectMsg(10.seconds, Ignore)
    }

    it("transports") {
      ingestor ! Ingest(TestRecord("test", "test", None, AckStrategy.NoAck), AckStrategy.NoAck)
      probe.expectMsg(Produce(TestRecord("test", "test", None, AckStrategy.NoAck), self, AckStrategy.NoAck))
    }
  }
}

case class TestRecord(destination: String, payload: String, key: Option[String], ackStrategy: AckStrategy) extends HydraRecord[String, String]
