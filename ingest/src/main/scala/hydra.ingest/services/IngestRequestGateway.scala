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
import hydra.core.http.ImperativeRequestContext
import hydra.core.ingest.HydraRequest
import hydra.ingest.marshallers.HydraIngestJsonSupport
import hydra.ingest.services.IngestRequestGateway.InitiateHttpRequest

import scala.concurrent.duration._

/**
  * A top (system) level actor to avoid creating one for every ingest request.
  * All this actor does is forward the requests to an instance of the IngestRequestHandler actor.
  *
  */
class IngestRequestGateway(registryPath: String) extends Actor with HydraIngestJsonSupport {

  private lazy val registry = context.actorSelection(registryPath).resolveOne()(Timeout(10.seconds))

  override def receive = {
    case InitiateHttpRequest(request, timeout, ctx) =>
      implicit val ec = context.dispatcher
      val fs = sender
      registry.map(r =>
        context.actorOf(HttpIngestionHandler.props(request, timeout, ctx, r)))
        .recover { case e: Exception => fs ! e }
  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case _ => Stop //stop ingestion IngestionRequestHandler always
    }
}

object IngestRequestGateway {

  case class InitiateHttpRequest(request: HydraRequest, timeout: FiniteDuration,
                                 ctx: ImperativeRequestContext)

  def props(registryPath: String) = Props(classOf[IngestRequestGateway], registryPath)

}