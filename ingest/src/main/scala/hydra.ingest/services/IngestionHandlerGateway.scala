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

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{OneForOneStrategy, _}
import akka.util.Timeout
import hydra.core.protocol.{InitiateHttpRequest, InitiateRequest}

import scala.concurrent.duration._

/**
  * A top (system) level actor to avoid creating one for every ingest request.
  * All this actor does is forward the requests to an instance of the IngestRequestHandler actor.
  *
  */
class IngestionHandlerGateway(registryPath: String)
    extends Actor
    with ActorLogging {

  private lazy val registry =
    context.actorSelection(registryPath).resolveOne()(Timeout(10.seconds))

  private implicit val ec = context.dispatcher

  override def receive = {
    case InitiateRequest(request, timeout, requestorOpt) =>
      val requestor = requestorOpt getOrElse sender
      ingest(
        registryRef =>
          DefaultIngestionHandler
            .props(request, registryRef, requestor, timeout),
        requestor
      )

    case InitiateHttpRequest(request, timeout, ctx) =>
      val requestor = sender
      ingest(
        registryRef =>
          HttpIngestionHandler.props(request, timeout, ctx, registryRef),
        requestor
      )
  }

  private def ingest(props: ActorRef => Props, requestor: ActorRef) = {
    registry
      .map(registryRef => context.actorOf(props(registryRef)))
      .recover { case e: Exception => requestor ! e }
  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case _ => Stop //stop ingestion IngestionRequestHandler always
    }
}

object IngestionHandlerGateway {

  val GroupName = "ingestion-handlers"

  def props(registryPath: String) =
    Props(classOf[IngestionHandlerGateway], registryPath)

}
