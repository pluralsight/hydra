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
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import hydra.common.config.ConfigSupport
import hydra.core.http.ImperativeRequestContext
import hydra.core.ingest.HydraRequest
import hydra.core.protocol._
import hydra.ingest.marshallers.IngestionJsonSupport
import hydra.ingest.protocol.IngestionReport

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
    Try(context.actorOf(IngestionSupervisor.props(request, timeout, registry))).recover {
      case e: Exception => self ! GenericHydraError(e)
    }
  }

  def receive = {
    case report: IngestionReport => complete(report)
    case ReceiveTimeout =>
      fail(errorWith(StatusCodes.custom(StatusCodes.RequestTimeout.intValue, s"No transport joined in ${timeout}.")))
    case e: HydraError => fail(errorWith(StatusCodes.InternalServerError))
    case _ => fail(errorWith(StatusCodes.BadRequest))
  }

  def complete(report: IngestionReport) = {
    ctx.complete(report.statusCode, report)
    stop(self)
  }

  def fail(obj: IngestionReport) = {
    ctx.complete(obj.statusCode, obj)
    stop(self)
  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case e => {
        fail(errorWith(StatusCodes.ServiceUnavailable))
        Stop
      }
    }

  private def errorWith(statusCode: StatusCode) = {
    IngestionReport(request.correlationId, request.metadata, Map.empty, statusCode.intValue())
  }
}

object IngestionRequestHandler extends ConfigSupport {

  import configs.syntax._

  val ingestionTimeout = applicationConfig.get[FiniteDuration]("ingestion.timeout").valueOrElse(3.seconds)

  def props(request: HydraRequest, registry: ActorRef, ctx: ImperativeRequestContext) =
    Props(classOf[IngestionRequestHandler], request, registry, ctx, ingestionTimeout)
}