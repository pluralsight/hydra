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
import hydra.ingest.IngestorInfo
import kamon.Kamon
import org.joda.time.DateTime

import scala.collection.mutable
import scala.concurrent.duration._

class IngestionSupervisor(
    request: HydraRequest,
    requestor: ActorRef,
    info: Seq[IngestorInfo],
    timeout: FiniteDuration
) extends Actor
    with ActorLogging {

  context.setReceiveTimeout(timeout)

  val start = DateTime.now()

  private val ingestors: mutable.Map[String, IngestorStatus] =
    new mutable.HashMap

  private val ingestTimer = Kamon.timer("ingestion").withoutTags().start()

  info.foreach { i =>
    ingestors.update(i.name, RequestPublished)
    context.actorSelection(i.path) ! Publish(request)
  }

  override def receive: Receive = timeOut orElse {
    case Join =>
      ingestors.update(ActorUtils.actorName(sender), IngestorJoined)
      sender ! Validate(request)

    case Ignore =>
      ingestors.remove(ActorUtils.actorName(sender))
      finishIfReady()

    case ValidRequest(record) =>
      sender ! Ingest(record, request.ackStrategy)

    case i: InvalidRequest =>
      context.system.eventStream.publish(
        InvalidRequestError(
          ActorUtils.actorName(sender),
          request,
          start,
          i.cause
        )
      )
      updateStatus(sender, i)

    case IngestorCompleted =>
      updateStatus(sender, IngestorCompleted)

    case IngestorTimeout =>
      updateStatus(sender, IngestorTimeout)

    case err: IngestorError =>
      val errorMsg = GenericIngestionError(
        ActorUtils.actorName(sender),
        err.cause,
        request,
        503
      )
      context.system.eventStream.publish(errorMsg)
      updateStatus(sender, err)
  }

  def updateStatus(ingestor: ActorRef, status: IngestorStatus) = {
    val name = ActorUtils.actorName(ingestor)
    ingestors.update(name, status)
    finishIfReady()
  }

  def timeOut: Receive = {

    case ReceiveTimeout =>
      //get status for ingestors
      val errorMsg =
        s"${request.correlationId}: Ack:${request.ackStrategy}; Validation: ${request.validationStrategy};" +
          s" Metadata:${request.metadata}; Payload: ${request.payload} Ingestors: ${ingestors.toString}; Timeout: $timeout"
      log.error(s"Ingestion timed out for request $errorMsg")
      context.system.eventStream.publish(
        IngestionTimedOut(request, start, timeout, ingestors.keys.mkString(","))
      )
      timeoutIngestors()
      stop(
        StatusCodes
          .custom(408, s"No ingestors completed the request in ${timeout}.")
      )
  }

  private def timeoutIngestors(): Unit = {
    ingestors
      .filter(_._2 != IngestorCompleted)
      .foreach(i => ingestors.update(i._1, IngestorTimeout))
  }

  private def finishIfReady(): Unit = {
    if (ingestors.isEmpty) {
      stop(StatusCodes.custom(404, s"No ingestors joined this request."))
    } else if (ingestors.values.filterNot(_.completed).isEmpty) {
      val status = ingestors
        .filter(_._2 != IngestorCompleted)
        .values
        .headOption
        .map(_.statusCode) getOrElse StatusCodes.OK
      stop(status)
    }
  }

  private def stop(status: StatusCode): Unit = {
    val report =
      IngestionReport(request.correlationId, ingestors.toMap, status.intValue())
    requestor ! report
    ingestTimer.stop()
    context.stop(self)
  }
}

object IngestionSupervisor {

  def props(
      request: HydraRequest,
      requestor: ActorRef,
      ingestors: Seq[IngestorInfo],
      timeout: FiniteDuration
  ): Props = {
    Props(classOf[IngestionSupervisor], request, requestor, ingestors, timeout)
  }
}
