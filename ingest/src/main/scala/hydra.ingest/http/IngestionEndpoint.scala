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

package hydra.ingest.http

import akka.actor._
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import configs.syntax._
import hydra.common.logging.LoggingAdapter
import hydra.core.http.HydraDirectives
import hydra.core.ingest.{CorrelationIdBuilder, RequestParams}
import hydra.core.marshallers.HydraJsonSupport
import hydra.core.protocol.InitiateHttpRequest
import hydra.ingest.bootstrap.HydraIngestorRegistryClient
import hydra.ingest.services.IngestionHandlerGateway

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}

/**
  * Created by alexsilva on 12/22/15.
  */
class IngestionEndpoint(implicit val system: ActorSystem, implicit val e: ExecutionContext)
  extends RoutedEndpoints with LoggingAdapter with HydraJsonSupport with HydraDirectives {

  import hydra.ingest.bootstrap.RequestFactories._

  implicit val mat = ActorMaterializer()

  //for performance reasons, we give this endpoint its own instance of the gateway
  private val registryPath = HydraIngestorRegistryClient.registryPath(applicationConfig)

  private val requestHandler = system.actorOf(IngestionHandlerGateway.props(registryPath),
    "ingestion_Http_handler_gateway")

  private val ingestTimeout = applicationConfig.get[FiniteDuration]("ingest.timeout")
    .valueOrElse(3.seconds)

  override val route: Route =
    post {
      requestEntityPresent {
        pathPrefix("ingest") {
          parameter("correlationId" ?) { correlationId =>
            pathEndOrSingleSlash {
              publishRequest(correlationId.getOrElse(cId))
            } ~ path(Segment) { ingestor =>
              publishRequest(correlationId.getOrElse(cId), Some(ingestor))
            }
          }
        }
      }
    }


  private def cId = CorrelationIdBuilder.generate()

  def publishRequest(correlationId: String, ingestor: Option[String] = None) = {
    extractRequestContext { ctx =>
      val reqFuture = createRequest[HttpRequest](correlationId, ctx.request)
      onSuccess(reqFuture) { req =>
        val request = ingestor
          .map(i => req.withMetadata(RequestParams.HYDRA_INGESTOR_PARAM -> i)).getOrElse(req)
        imperativelyComplete { ctx =>
          requestHandler ! InitiateHttpRequest(request, ingestTimeout, ctx)
        }
      }
    }
  }
}