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
import akka.http.scaladsl.model.{HttpHeader, StatusCodes}
import akka.http.scaladsl.server.Route
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import configs.syntax._
import hydra.common.logging.LoggingAdapter
import hydra.core.http.HydraDirectives
import hydra.core.marshallers.{GenericServiceResponse, HydraJsonSupport}
import hydra.ingest.bootstrap.HydraIngestorRegistry
import hydra.ingest.ws.IngestionSocket

/**
  * Created by alexsilva on 12/22/15.
  */
class IngestionWebSocketEndpoint(implicit val system: ActorSystem, implicit val actorRefFactory: ActorRefFactory)
  extends RoutedEndpoints with LoggingAdapter with HydraJsonSupport with HydraDirectives with HydraIngestorRegistry {

  val enabled = applicationConfig.get[Boolean]("ingest.websocket.enabled").valueOrElse(false)

  override val route: Route =
    path("ws-ingest" / Segment) { label =>
      extractHydraHeaders { headers =>
        if (enabled) {
          handleWebSocketMessages(createSocket(label, headers))
        }
        else {
          complete(StatusCodes.Conflict, GenericServiceResponse(409, "Websocket not available."))
        }
      }
    }

  private def createSocket(label: String, headers: Seq[HttpHeader]) =
    IngestionSocket(headers.map(h => h.name().toUpperCase -> h.value).toMap).ingestionWSFlow(label)

  private val extractHydraHeaders = extract(_.request.headers.filter(_.lowercaseName.startsWith("hydra")))
}
