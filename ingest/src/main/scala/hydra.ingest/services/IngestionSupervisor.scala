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
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import hydra.common.util.ActorUtils
import hydra.core.ingest._
import hydra.core.protocol._
import hydra.ingest.services.IngestorRegistry.{FindAll, FindByName, LookupResult}
import org.joda.time.DateTime

import scala.collection.mutable
import scala.concurrent.duration._


class IngestionSupervisor(request: HydraRequest, timeout: FiniteDuration, registry: ActorRef) extends Actor
  with ActorLogging {

  context.setReceiveTimeout(timeout)

  val start = DateTime.now()

  private val ingestors: mutable.Map[String, IngestorStatus] = new mutable.HashMap

  private val targetIngestor = request.metadataValue(RequestParams.HYDRA_INGESTOR_PARAM)

  override def preStart(): Unit = {
    targetIngestor match {
      case Some(ingestor) => registry ! FindByName(ingestor)
      case None => registry ! FindAll
    }
  }

  override def receive = waitingForIngestors


  def waitingForIngestors: Receive = timeOut orElse {
    case LookupResult(Nil) =>
      val errorCode = targetIngestor
        .map(i => StatusCodes.custom(404, s"No ingestor named $i was found in the registry."))
        .getOrElse(StatusCodes.BadRequest)

      stop(errorCode)

    case LookupResult(ings) =>
      context.become(ingesting)

      ings.foreach { i =>
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

    case i: InvalidRequest =>
      context.system.eventStream.publish(HydraIngestionError(ActorUtils.actorName(sender), i.error, request))
      updateStatus(sender, i)

    case IngestorCompleted =>
      updateStatus(sender, IngestorCompleted)

    case IngestorTimeout =>
      updateStatus(sender, IngestorTimeout)

    case WaitingForAck =>
      updateStatus(sender, WaitingForAck)

    case err: IngestorError =>
      context.system.eventStream.publish(HydraIngestionError(ActorUtils.actorName(sender), err.error, request))
      updateStatus(sender, err)
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
      stop(StatusCodes.custom(408, s"No ingestors completed the request in ${timeout}."))
  }

  private def timeoutIngestors(): Unit = {
    ingestors.filter(_._2 != IngestorCompleted).foreach(i => ingestors.update(i._1, IngestorTimeout))
  }

  private def finishIfReady(): Unit = {
    if (ingestors.isEmpty) {
      stop(StatusCodes.custom(404, s"No ingestors joined this request."))
    }
    else if (ingestors.values.filterNot(_.completed).isEmpty) {
      val status = ingestors.filter(_._2 != IngestorCompleted).values.headOption
        .map(_.statusCode) getOrElse StatusCodes.OK
      stop(status)
    }
  }

  private def stop(status: StatusCode): Unit = {
    val replyTo = request.metadata.find(_._1.equalsIgnoreCase(RequestParams.REPLY_TO)).map(_._2)
    context.parent ! IngestionReport(request.correlationId, ingestors.toMap, status.intValue(), replyTo)
    context.stop(self)
  }
}

object IngestionSupervisor {

  def props(request: HydraRequest, timeout: FiniteDuration, ingestorRegistry: ActorRef): Props =
    Props(classOf[IngestionSupervisor], request, timeout, ingestorRegistry)
}
