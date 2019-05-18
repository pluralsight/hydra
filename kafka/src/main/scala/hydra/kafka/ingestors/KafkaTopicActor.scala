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

import akka.actor.{Actor, Timers}
import com.typesafe.config.Config
import hydra.kafka.ingestors.KafkaTopicActor.{GetTopicRequest, Initialize}
import hydra.kafka.util.KafkaUtils
import org.apache.kafka.clients.Metadata
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata
import org.joda.time.DateTime
import scalacache.guava.GuavaCache

import scala.collection.immutable.Map
import scala.util.{Success, Try}

class KafkaTopicActor(cfg: Config) extends Actor with Timers {

  private val kUtils = KafkaUtils(cfg)

  self ! ExceptionInInitializerError

  override def receive: Receive = {
    case Initialize => context.become(withTopics(kUtils.topicNames().getOrElse(Seq.empty)))

  }

  override def withTopics(topics: Seq[String]): Receive = {
    case GetTopicRequest(topic) => topics.find(_ == topic).map(GetTopicResponse())
  }


  private def getTopic(topic: String): Try[Map[String, Seq[PartitionInfo]]] = {
    implicit val cache = KafkaIngestor.topicCache
    cache.get(topic) match {
      case Success(result) if result.
      case None =>
    }
  }
}

object KafkaTopicActor {

  case class TopicLookupResult(topic: String, lookupTime: DateTime, result: Boolean)

  case object Initialize

  case class GetTopicRequest(topic: String)

  case class GetTopicResponse(topicMetadata: Metadata)

}


