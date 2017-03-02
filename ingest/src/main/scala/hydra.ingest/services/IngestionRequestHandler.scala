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
import hydra.common.config.ConfigSupport
import hydra.core.http.ImperativeRequestContext
import hydra.core.ingest.HydraRequest
import hydra.core.protocol.{GenericHydraError, HydraError}
import hydra.ingest.marshallers.{IngestionErrorResponse, IngestionJsonSupport, IngestionResponse}
import hydra.ingest.protocol.{IngestionCompleted, IngestionStatus}
import hydra.ingest.services.IngestionSupervisor.InitiateIngestion

import scala.concurrent.duration._
import scala.util.Try

/**
  * Created by alexsilva on 12/22/15.
  */
class IngestionRequestHandler(request: HydraRequest, registry: ActorRef,
                              ctx: ImperativeRequestContext, timeout: FiniteDuration) extends Actor
  with IngestionJsonSupport {

  import context._

  context.setReceiveTimeout(timeout)

  override def preStart(): Unit = {
    Try(context.actorOf(IngestionSupervisor.props(registry, request, timeout))).map(_ ! InitiateIngestion).recover {
      case e: Exception => self ! GenericHydraError(e)
    }
  }


  def receive = {
    case IngestionCompleted(h: IngestionStatus) =>
      complete(IngestionResponse(h))
    case ReceiveTimeout =>
      fail(IngestionErrorResponse.ingestionTimedOut(s"No transport joined within ${timeout}."))
    case e: HydraError =>
      fail(IngestionErrorResponse.serverError(e.cause.getMessage))
    case _ =>
      fail(IngestionErrorResponse.requestError("Invalid request."))
  }

  def complete(obj: IngestionResponse) = {
    ctx.complete(obj.status, obj.result)
    stop(self)
  }

  def fail(obj: IngestionErrorResponse) = {
    ctx.complete(obj.status, obj)
    stop(self)
  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case e => {
        fail(IngestionErrorResponse.serverError(e.getMessage))
        Stop
      }
    }
}

object IngestionRequestHandler extends ConfigSupport {

  import configs.syntax._

  val ingestionTimeout = applicationConfig.get[FiniteDuration]("ingestion.timeout").valueOrElse(3.seconds)

  def props(request: HydraRequest, registry: ActorRef, ctx: ImperativeRequestContext) =
    Props(classOf[IngestionRequestHandler], request, registry, ctx, ingestionTimeout)
}