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

package hydra.ingest.endpoints

import akka.actor._
import akka.http.scaladsl.model.StatusCodes.ServiceUnavailable
import akka.http.scaladsl.server.{ExceptionHandler, RequestContext, Route}
import akka.pattern.ask
import akka.util.Timeout
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import hydra.common.akka.ActorUtils
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.core.http.HydraDirectives
import hydra.core.ingest.{HydraRequest, HydraRequestMedatata}
import hydra.core.marshallers.GenericServiceResponse
import hydra.ingest.marshallers.IngestionJsonSupport
import hydra.ingest.services.IngestorRegistry
import hydra.ingest.services.IngestorRegistry.{GetIngestors, RegisteredIngestors}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Created by alexsilva on 12/22/15.
  */
class IngestorRegistryEndpoint(implicit system: ActorSystem, implicit val actorRefFactory: ActorRefFactory)
  extends RoutedEndpoints with LoggingAdapter with IngestionJsonSupport with HydraDirectives with ConfigSupport {

  implicit val timeout = 3.seconds

  implicit val askTimeout = Timeout(timeout)

  lazy val registryHandle: Future[ActorRef] =
    actorRefFactory.actorSelection(s"/user/service/${ActorUtils.actorName(classOf[IngestorRegistry])}")
      .resolveOne()

  override val route: Route =
    path("ingestors" ~ Slash.?) {
      get {
        onComplete(registryHandle) {
          case Success(registry) =>
            onSuccess(registry ? GetIngestors) {
              case response: RegisteredIngestors => complete(response.ingestors)
            }
          case Failure(ex) => throw ex
        }
      }

    }

  //todo: add retry strategy, etc. stuff
  private def buildRequest(destination: String, payload: String, ctx: RequestContext) = {
    //here for backwards compatibility
    val metadata: List[HydraRequestMedatata] = List(ctx.request.headers.map(header =>
      HydraRequestMedatata(header.name.toLowerCase, header.value)): _*)
    HydraRequest(destination, payload, metadata)
  }

  val excptHandler = ExceptionHandler {
    case e: IllegalArgumentException => complete(GenericServiceResponse(400, e.getMessage))
    case e: Exception => complete(GenericServiceResponse(ServiceUnavailable.intValue, e.getMessage))
  }
}
