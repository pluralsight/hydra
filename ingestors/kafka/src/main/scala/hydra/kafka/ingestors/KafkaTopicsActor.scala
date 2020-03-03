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

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.actor.Status.Failure
import akka.actor.{Actor, Props, Stash, Timers}
import akka.pattern.pipe
import com.typesafe.config.Config
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import org.apache.kafka.clients.admin.Admin
import org.joda.time.DateTime

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

object KafkaTopicsActor {

  case object TopicsTimer

  case object RefreshTopicList

  case class GetTopicRequest(topic: String)

  case class GetTopicResponse(
      topic: String,
      lookupDate: DateTime,
      exists: Boolean
  )

  case class GetTopicsResponse(topics: Seq[String])

  case class GetTopicsFailure(cause: Throwable)

  def fetchTopics(
      createAdminClient: () => Admin,
      kafkaTimeoutSeconds: Long
  ): Future[GetTopicsResponse] = {
    Future.fromTry {
      Try(createAdminClient()).map { c =>
        try {
          val t = c
            .listTopics()
            .names
            .get(kafkaTimeoutSeconds, TimeUnit.SECONDS)
            .asScala
            .toSeq
          GetTopicsResponse(t)
        } finally {
          Try(c.close(Duration.ofSeconds(kafkaTimeoutSeconds)))
        }
      }
    }
  }

  def props(
      cfg: Config,
      checkInterval: FiniteDuration = 5.seconds,
      kafkaTimeoutSeconds: Long = 2
  ): Props =
    Props(new KafkaTopicsActor(cfg, checkInterval, kafkaTimeoutSeconds))

}

class KafkaTopicsActor(
    cfg: Config,
    checkInterval: FiniteDuration,
    kafkaTimeoutSeconds: Long
) extends Actor
    with Timers
    with LoggingAdapter
    with Stash {

  import KafkaTopicsActor._

  self ! RefreshTopicList

  timers.startTimerAtFixedRate(TopicsTimer, RefreshTopicList, checkInterval)

  implicit val ec = context.dispatcher

  private def createAdminClient(): Admin =
    Admin.create(ConfigSupport.toMap(cfg).asJava)

  val initialReceive: Receive = {
    case RefreshTopicList =>
      pipe(fetchTopics(createAdminClient, kafkaTimeoutSeconds)) to self

    case GetTopicsResponse(topics) =>
      context.become(withTopics(topics) orElse handleFailure)
      unstashAll()

    case GetTopicRequest(_) => stash()

  }

  override def receive: Receive = initialReceive orElse handleFailure

  private def withTopics(topicList: Seq[String]): Receive = {
    case GetTopicRequest(topic) =>
      val topicR = topicList.find(_ == topic)
      sender ! GetTopicResponse(topic, DateTime.now, topicR.isDefined)

    case RefreshTopicList =>
      pipe(fetchTopics(createAdminClient, kafkaTimeoutSeconds)) to self

    case GetTopicsResponse(topics) =>
      context.become(withTopics(topics) orElse handleFailure)
  }

  private def handleFailure: Receive = {
    case Failure(ex) =>
      log.error(
        s"Error occurred while attempting to retrieve topics: ${ex.getMessage}",
        ex: Throwable
      )
      context.system.eventStream.publish(GetTopicsFailure(ex))
  }

  override def postStop(): Unit = {
    timers.cancel(TopicsTimer)
  }
}
