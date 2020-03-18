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

package hydra.ingest.services

import akka.actor._
import hydra.common.config.ConfigSupport
import hydra.core.ingest.{HydraRequest, IngestionReport}
import hydra.core.protocol.HydraApplicationError

import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * Created by alexsilva on 12/22/15.
  */
class DefaultIngestionHandler(
    val request: HydraRequest,
    val registry: ActorRef,
    val requestor: ActorRef,
    val timeout: FiniteDuration
) extends Actor
    with IngestionHandler
    with ActorLogging {

  private implicit val ec = context.dispatcher

  // private val mediator = DistributedPubSub(context.system).mediator

  //mediator ! Subscribe(HydraRequestPublisher.TopicName, Some(HydraRequestPublisher.GroupName), self)

  override def complete(report: IngestionReport): Unit = {
    requestor ! report
    context.stop(self)
  }

  override def fail(e: Throwable): Unit = {
    requestor ! HydraApplicationError(e)
    context.stop(self)
  }
}

object DefaultIngestionHandler extends ConfigSupport {

  def props(
      request: HydraRequest,
      registry: ActorRef,
      requestor: ActorRef
  ): Props = {
    import ConfigSupport._

    val timeout = applicationConfig
      .getDurationOpt("ingestion.timeout")
      .getOrElse(3.seconds)

    props(request, registry, requestor, timeout)
  }

  def props(
      request: HydraRequest,
      registry: ActorRef,
      requestor: ActorRef,
      timeout: FiniteDuration
  ): Props = {
    Props(new DefaultIngestionHandler(request, registry, requestor, timeout))
  }
}
