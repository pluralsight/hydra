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
import hydra.common.util.ActorUtils
import hydra.core.ingest._
import hydra.core.protocol._
import hydra.ingest.protocol.IngestionReport
import hydra.ingest.services.IngestorRegistry.{FindAll, FindByName, LookupResult}
import org.joda.time.DateTime

import scala.collection.mutable
import scala.concurrent.duration._


class IngestionSupervisor(request: HydraRequest, timeout: FiniteDuration, registry: ActorRef) extends Actor
  with ActorLogging {

  context.setReceiveTimeout(timeout)

  val start = DateTime.now()

  private var ingestors: mutable.Map[String, IngestorStatus] = new mutable.HashMap

  override def preStart(): Unit = {
    request.metadataValue(IngestionParams.HYDRA_INGESTOR_TARGET_PARAM) match {
      case Some(ingestor) => registry ! FindByName(ingestor)
      case None => registry ! FindAll
    }
  }

  override def receive = waitingForIngestors


  def waitingForIngestors: Receive = timeOut orElse {
    case r: LookupResult =>
      context.become(ingesting)

      r.ingestors.foreach { i =>
        ingestors.put(i.name, RequestPublished)
        context.actorSelection(i.path) ! Publish(request)
      }
  }

  def ingesting: Receive = timeOut orElse {
    case Join =>
      ingestors.update(ActorUtils.actorName(sender), IngestorJoined)
      sender ! Validate(request)

    case Ignore =>
      ingestors.remove(ActorUtils.actorName(sender))
      finishIfReady()

    case ValidRequest =>
      sender ! Ingest(request)

    case error: InvalidRequest =>
      context.system.eventStream.publish(error)
      updateStatus(sender, error)

    case IngestorCompleted =>
      updateStatus(sender, IngestorCompleted)

    case IngestorTimeout =>
      updateStatus(sender, IngestorTimeout)

    case error: IngestorError =>
      updateStatus(sender, IngestorTimeout)

    case err: HydraIngestionError =>
      updateStatus(sender, IngestorError(err.cause))
  }

  def updateStatus(ingestor: ActorRef, status: IngestorStatus) = {
    val name = ActorUtils.actorName(ingestor)
    ingestors.update(name, status)
    finishIfReady()
  }

  def timeOut: Receive = {
    case ReceiveTimeout =>
      log.error(s"Ingestion timed out for $request")
      timeoutIngestors()
  }


  private def timeoutIngestors(): Unit = {
    ingestors.filter(_._2 != IngestorCompleted).foreach(i => ingestors.update(i._1, IngestorTimeout))
  }

  private def finishIfReady() = {
    val ready = ingestors.values.filterNot(_.completed).isEmpty
    if (ready) {
      context.parent ! IngestionReport(request, ingestors.toMap)
      context.stop(self)
    }
  }

}

object IngestionSupervisor {
  def props(request: HydraRequest, timeout: FiniteDuration, ingestorRegistry: ActorRef): Props =
    Props(classOf[IngestionSupervisor], request, timeout, ingestorRegistry)
}
