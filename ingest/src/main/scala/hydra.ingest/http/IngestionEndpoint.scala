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
import akka.http.scaladsl.server.{ExceptionHandler, Rejection, Route}
import hydra.common.config.ConfigSupport._
import hydra.common.util.Futurable
import hydra.core.http.RouteSupport
import hydra.core.ingest.{CorrelationIdBuilder, HydraRequest, IngestionReport, RequestParams}
import hydra.core.marshallers.GenericError
import hydra.core.protocol.{IngestorCompleted, InitiateHttpRequest, RequestPublished}
import hydra.ingest.bootstrap.HydraIngestorRegistryClient
import hydra.ingest.services.IngestionHandlerGateway

import scala.concurrent.duration.{FiniteDuration, _}

/**
  * Created by alexsilva on 12/22/15.
  */
class IngestionEndpoint[F[_]: Futurable](
                                          alternateIngestFlowEnabled: Boolean,
                                          ingestionFlow: IngestionFlow[F],
                                          useOldIngestUAContains: Set[String],
                                          requestHandlerPathName: Option[String] = None
                                        )(implicit system: ActorSystem) extends RouteSupport with HydraIngestJsonSupport {

  import hydra.ingest.bootstrap.RequestFactories._

  //for performance reasons, we give this endpoint its own instance of the gateway
  private val registryPath =
    HydraIngestorRegistryClient.registryPath(applicationConfig)

  private val requestHandler = system.actorOf(
    IngestionHandlerGateway.props(registryPath),
    requestHandlerPathName.getOrElse("ingestion_Http_handler_gateway")
  )

  private val ingestTimeout = applicationConfig
    .getDurationOpt("ingest.timeout")
    .getOrElse(500.millis)

  override val route: Route =
    pathPrefix("ingest") {
      pathEndOrSingleSlash {
        handleExceptions(exceptionHandler) {
          post {
            requestEntityPresent {
              publishRequest
            }
          } ~ deleteRequest
        }
      }
    }

  private def deleteRequest = delete {
    headerValueByName(RequestParams.HYDRA_RECORD_KEY_PARAM)(_ => publishRequest)
  }

  private def cId = CorrelationIdBuilder.generate()

  private def useAlternateIngestFlow(request: HydraRequest): Boolean = {
    request.metadata.find(e => e._1.equalsIgnoreCase("User-Agent")) match {
      case Some(ua) if useOldIngestUAContains.exists(ua._2.startsWith) => false
      case _ => true
    }
  }

  private def publishRequest = parameter("correlationId" ?) { cIdOpt =>
    extractRequest { req =>
      onSuccess(createRequest[HttpRequest](cIdOpt.getOrElse(cId), req)) { hydraRequest =>
        if (alternateIngestFlowEnabled && useAlternateIngestFlow(hydraRequest)) {
          onSuccess(Futurable[F].unsafeToFuture(ingestionFlow.ingest(hydraRequest))) {
            complete(IngestionReport(hydraRequest.correlationId, Map("kafka_ingestor" -> IngestorCompleted), 200))
          }
        } else {
          imperativelyComplete { ctx =>
            requestHandler ! InitiateHttpRequest(
              hydraRequest,
              ingestTimeout,
              ctx
            )
          }
        }
      }
    }
  }

  private def exceptionHandler = ExceptionHandler {
    case e: IllegalArgumentException =>
      if (applicationConfig
            .getBooleanOpt("hydra.ingest.shouldLog400s")
            .getOrElse(false)) {
        log.error("Ingestion 400 ERROR: " + e.getMessage)
      }
      complete(400, GenericError(400, e.getMessage))
  }
}

case object DeleteDirectiveNotAllowedRejection extends Rejection
