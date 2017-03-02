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

package hydra.ingest.services

import akka.actor.{Actor, Props}
import com.flyberrycapital.slack.SlackClient
import hydra.common.config.ConfigSupport
import hydra.core.notification.HydraEvent
import hydra.ingest.services.SlackClientActor.PublishToChannel

/**
  * Created by alexsilva on 9/30/16.
  */
class SlackClientActor(token: String) extends Actor with ConfigSupport {

  private val slack = new SlackClient(token)

  //todo: configurable or part of message
  private val defaultChannel = "#hydra-ops"

  override def receive: Receive = {
    case PublishToChannel(channel, message) => slack.chat.postMessage(channel, message)
    case i: HydraEvent[_] => slack.chat.postMessage(defaultChannel, "Ingestion Error:" + i.source.toString)

  }
}

object SlackClientActor {

  case class PublishToChannel(channel: String = "#hydra-ops", message: String)

  def props(token: String) = Props(classOf[SlackClientActor], token)

}