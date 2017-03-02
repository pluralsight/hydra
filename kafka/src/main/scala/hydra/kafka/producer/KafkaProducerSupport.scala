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

package hydra.kafka.producer

import akka.actor.Actor
import akka.util.Timeout
import configs.syntax._
import hydra.common.config.ConfigSupport

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Mix this trait in to get a KafkaProducerActor automatically looked up.
  *
  * Created by alexsilva on 12/29/15.
  */
trait KafkaProducerSupport extends ConfigSupport {
  this: Actor =>

  private implicit val timeout = Timeout(1.second)

  val path = applicationConfig.get[String]("kafka.supervisor.path")
    .valueOrElse("/user/service/kafka_producer_supervisor")

  val kafkaProducer = Await.result(context.actorSelection(path).resolveOne(), 1.seconds)
}