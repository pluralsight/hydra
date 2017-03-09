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
import hydra.core.ingest._
import hydra.core.protocol._
import hydra.ingest.protocol.IngestionInfo
import hydra.ingest.services.IngestionSupervisor1.InitiateIngestion
import hydra.ingest.services.IngestorRegistry.{IngestorLookupResult, Lookup}

import scala.concurrent.duration._

/**
  * This actor gets instantiated for every ingestion request, so we can keep stats and other metadata.
  *
  * @param request  The HydraRequest
  * @param registry A reference to the ingestor registry
  * @param timeout  Duration after which the ingestion timesout.
  *
  *                 Created by alexsilva on 11/22/15.
  */
class IngestionSupervisor1(request: HydraRequest, registry: ActorRef, timeout: FiniteDuration) extends Actor with ActorLogging {

  context.setReceiveTimeout(timeout)

  val status = new IngestionInfo

  def initiate: Receive = {
    case InitiateIngestion(request) =>
      request.metadataValue(IngestionParams.HYDRA_INGESTOR_TARGET_PARAM) match {
        case Some(ingestor) => registry ! Lookup(ingestor)
        case None => registry ! Publish(request)
      }

    case IngestorLookupResult(name, ref) =>
      ref match {
        case Some(ingestor) => self.tell(Join, ingestor)
        case None => status.set(name, new InvalidRequest(s"Ingestor $name not found."))
      }
  }


  def receive = ???
}

object IngestionSupervisor1 extends ConfigSupport {

  def props(handlerRegistry: ActorRef, request: HydraRequest, timeout: FiniteDuration): Props =
    Props(classOf[IngestionSupervisor], request, handlerRegistry, timeout)

  case class InitiateIngestion(request: HydraRequest) extends HydraMessage

}
