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
import hydra.core.http.ImperativeRequestContext
import hydra.core.ingest.{HydraRequest, IngestionReport}
import hydra.ingest.http.HydraIngestJsonSupport

import scala.concurrent.duration._

/**
  * Created by alexsilva on 12/22/15.
  */
class HttpIngestionHandler(
    val request: HydraRequest,
    val timeout: FiniteDuration,
    val registry: ActorRef,
    ctx: ImperativeRequestContext
) extends Actor
    with IngestionHandler
    with HydraIngestJsonSupport {

  def complete(report: IngestionReport) = {
    ctx.complete(report.statusCode, report)
    context.stop(self)
  }

  def fail(e: Throwable) = {
    ctx.failWith(e)
    context.stop(self)
  }
}

object HttpIngestionHandler {

  def props(
      req: HydraRequest,
      timeout: FiniteDuration,
      ctx: ImperativeRequestContext,
      registry: ActorRef
  ): Props = {
    Props(classOf[HttpIngestionHandler], req, timeout, registry, ctx)
  }

}
