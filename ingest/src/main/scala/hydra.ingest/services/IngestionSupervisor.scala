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
import com.github.vonnagy.service.container.service.ServicesManager
import hydra.common.config.ConfigSupport
import hydra.core.ingest._
import hydra.core.protocol._
import hydra.ingest.protocol.{IngestionCompleted, IngestionStatus}
import hydra.ingest.services.IngestionErrorHandler.HandleError
import hydra.ingest.services.IngestionSupervisor.InitiateIngestion
import hydra.ingest.services.IngestorRegistry.{IngestorLookupResult, Lookup}

import scala.concurrent.duration._

/**
  * This actor gets instantiated for every request, so we can keep stats and other metadata.
  * .
  * Created by alexsilva on 11/22/15.
  */
class IngestionSupervisor(request: HydraRequest, registry: ActorRef, timeout: FiniteDuration) extends Actor with ActorLogging {

  context.setReceiveTimeout(timeout)

  val status = IngestionStatus(request)

  override def receive: Receive = timedOut orElse {
    case InitiateIngestion =>
      request.metadataValue(IngestionParams.HYDRA_INGESTOR_TARGET_PARAM) match {
        case Some(ingestor) => registry ! Lookup(ingestor)
        case None => registry ! Publish(request)
      }

    case IngestorLookupResult(name, ingestorOpt) =>
      ingestorOpt match {
        case Some(ingestor) => self.tell(Join, ingestor)
        case None => status.set(name, new InvalidRequest(s"Ingestor $name not found."))
      }

    case Join =>
      status.joined(iname(sender))
      sender ! Validate(request)

    case Ignore => //how to handle?

    case ValidRequest => sender ! Ingest(request)

    case InvalidRequest(error) =>
      implicit val ec = context.dispatcher
      ServicesManager.findService("ingestion_error_handler")(context.system)
        .foreach(_ ! HandleError(sender, request, error))
      status.set(iname(sender), InvalidRequest(error))
      finishIfReady()

    case IngestorCompleted =>
      status.set(iname(sender), IngestorCompleted)
      finishIfReady()

    case IngestorError(ex) =>
      status.set(iname(sender), IngestorError(ex))
      finishIfReady()

    case IngestorTimeout =>
      status.set(iname(sender), IngestorTimeout)
      finishIfReady()

    case err: HydraIngestionError =>
      status.set(iname(sender), IngestorError(err.cause))
      finishIfReady()

    case other =>
      log.error("Received unknown message:" + other)
      status.set(iname(sender),
        IngestorError(new IllegalArgumentException(s"${sender.path.name} sent unknown message: $other")))
      finishIfReady()
  }

  def timedOut: Receive = {
    case ReceiveTimeout =>
      log.error(s"Ingestion timed out for $request")
      status.timeOut()
      doFinish()
  }

  private def finishIfReady() = {
    if (status.isComplete) doFinish()
  }

  private def doFinish() = {
    context.parent ! IngestionCompleted(status)
    context.stop(self)
  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case _: Exception => SupervisorStrategy.Escalate
    }

  private def iname(ingestor: ActorRef) = {
    val elems = ingestor.path.elements.toList
    if (elems.last.startsWith("$")) elems.takeRight(2)(0) else elems.last
  }
}

object IngestionSupervisor extends ConfigSupport {

  def props(handlerRegistry: ActorRef, request: HydraRequest, timeout: FiniteDuration): Props =
    Props(classOf[IngestionSupervisor], request, handlerRegistry, timeout)

  case object InitiateIngestion extends HydraMessage

}
