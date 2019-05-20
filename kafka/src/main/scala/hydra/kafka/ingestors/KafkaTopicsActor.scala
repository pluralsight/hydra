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

package hydra.kafka.ingestors

import akka.actor.{Actor, Props, Stash, Timers}
import com.typesafe.config.Config
import hydra.common.logging.LoggingAdapter
import hydra.kafka.ingestors.KafkaTopicActor.{GetTopicRequest, GetTopicResponse, RefreshTopicList, TopicsTImer}
import hydra.kafka.util.KafkaUtils
import org.joda.time.DateTime

import scala.concurrent.duration._

class KafkaTopicsActor(cfg: Config) extends Actor
  with Timers
  with LoggingAdapter
  with Stash {

  private val kUtils = KafkaUtils(cfg)

  self ! RefreshTopicList

  timers.startPeriodicTimer(TopicsTImer, RefreshTopicList, 1 seconds)

  private def topics = kUtils.topicNames()
    .recover { case e => log.error("Unable to load Kafka topics.", e); Seq.empty }.get

  override def receive: Receive = {
    case RefreshTopicList =>
      context.become(withTopics(topics))
      unstashAll()
    case GetTopicRequest(_) => stash()
  }

  private def withTopics(topics: Seq[String]): Receive = {
    case GetTopicRequest(topic) =>
      val topicR = topics.find(_ == topic)
      sender ! GetTopicResponse(topic, DateTime.now, topicR.isDefined)

    case RefreshTopicList => println(kUtils.topicNames()
      .recover { case e => log.error("Unable to load Kafka topics.", e); Seq.empty }.get);

      context.become(withTopics(topics))
  }

  override def postStop(): Unit = {
    timers.cancel(TopicsTImer)
  }
}

object KafkaTopicActor {

  case object TopicsTImer

  case object RefreshTopicList

  case class GetTopicRequest(topic: String)

  case class GetTopicResponse(topic: String, lookupDate: DateTime, exists: Boolean)

  def props(config: Config) = Props(classOf[KafkaTopicsActor], config)

}


