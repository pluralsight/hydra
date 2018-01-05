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
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe
import akka.util.Timeout
import hydra.core.http.ImperativeRequestContext
import hydra.core.ingest.HydraRequest
import hydra.ingest.services.IngestionHandlerGateway.{InitiateHttpRequest, InitiateRequest}

import scala.concurrent.duration._

/**
  * A top (system) level actor to avoid creating one for every ingest request.
  * All this actor does is forward the requests to an instance of the IngestRequestHandler actor.
  *
  */
class IngestionHandlerGateway(registryPath: String) extends Actor with ActorLogging {

  private lazy val registry = context.actorSelection(registryPath).resolveOne()(Timeout(10.seconds))

  private implicit val ec = context.dispatcher

  override def receive = {
    case InitiateRequest(request, timeout) =>
      val fs = sender
      ingest(r => DefaultIngestionHandler.props(request, r, fs, timeout), fs)

    case InitiateHttpRequest(request, timeout, ctx) =>
      val fs = sender
      ingest(r => HttpIngestionHandler.props(request, timeout, ctx, r), fs)
  }

  private def ingest(props: (ActorRef) => Props, requestor: ActorRef) = {
    registry.map(r => context.actorOf(props(r))).recover { case e: Exception => requestor ! e }
  }


  /**
    * Only in clustered environments.
    */
  override def preStart():Unit = {
    val isClustered = context.system.settings.ProviderClass == "akka.cluster.ClusterActorRefProvider"
    if (isClustered) {
      log.debug("Initialized DistributedPubSub for {}", IngestionHandlerGateway.TopicName)
      val mediator = DistributedPubSub(context.system).mediator
      mediator ! Subscribe(IngestionHandlerGateway.TopicName,
        Some(IngestionHandlerGateway.GroupName), self)
    }
  }


  override val supervisorStrategy =
    OneForOneStrategy() {
      case _ => Stop //stop ingestion IngestionRequestHandler always
    }
}

object IngestionHandlerGateway {

  val TopicName = "hydra-ingest"

  val GroupName = "ingestion-handlers"

  case class InitiateHttpRequest(request: HydraRequest, timeout: FiniteDuration,
                                 ctx: ImperativeRequestContext)

  case class InitiateRequest(request: HydraRequest, timeout: FiniteDuration)

  def props(registryPath: String) = Props(classOf[IngestionHandlerGateway], registryPath)

}