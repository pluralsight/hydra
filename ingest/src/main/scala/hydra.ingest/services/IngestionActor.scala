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
import akka.util.Timeout
import hydra.common.config.ActorConfigSupport
import hydra.core.ingest.{HydraRequest, IngestionReport, RequestParams}
import hydra.core.protocol.{IngestionError, InitiateRequest}
import hydra.ingest.bootstrap.HydraIngestorRegistry

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
  * Created by alexsilva on 12/22/15.
  */
class IngestionActor extends Actor with ActorConfigSupport with ActorLogging with HydraIngestorRegistry {

  import configs.syntax._

  override val system = context.system

  private val ingestionTimeout = applicationConfig.get[FiniteDuration]("ingestion.timeout").valueOrElse(3.seconds)

  private implicit val timeout = Timeout(ingestionTimeout)

  private implicit val ec = context.dispatcher

  override def receive: Receive = {
    case InitiateRequest(r) =>
      val requestor = sender
      publishRequest(r, requestor)

    case r: IngestionReport =>
      context.stop(sender)
      r.metadata.find(_._1.equalsIgnoreCase(RequestParams.REPLY_TO)).foreach { replyTo =>
        Try(context.actorSelection(replyTo._2) ! r)
          .recover { case e => log.error(s"Unable to send reply back to ${receive}: ${e.getMessage}") }
      }
    case ReceiveTimeout =>
      log.error(s"$thisActorName: Received timeout.")
      context.stop(self)

    case other =>
      val msg = s"${other} is not expected at this state."
      log.warning(msg)
      sender ! IngestionError(new IllegalArgumentException(msg))
  }

  private def publishRequest(r: HydraRequest, requestor: ActorRef) = {
    ingestorRegistry onComplete {
      case Success(registry) =>
        context.actorOf(IngestionSupervisor.props(r, ingestionTimeout, registry))
      case Failure(ex) =>
        requestor ! IngestionError(ex)
    }
  }
}

