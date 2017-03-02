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
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import hydra.common.logging.LoggingAdapter
import hydra.core.http.HydraDirectives
import hydra.core.ingest.{HydraRequest, HydraRequestMedatata, IngestionParams}
import hydra.core.marshallers.{GenericServiceResponse, HydraJsonSupport}
import hydra.ingest.HydraIngestorRegistry
import hydra.ingest.services.IngestionRequestHandler

import scala.util.{Failure, Success}

/**
  * Created by alexsilva on 12/22/15.
  */
class IngestorEndpoint(implicit val system: ActorSystem, implicit val actorRefFactory: ActorRefFactory)
  extends RoutedEndpoints with LoggingAdapter with HydraJsonSupport with HydraDirectives with HydraIngestorRegistry {

  override val route: Route =
    post {
      requestEntityPresent {
        pathPrefix("ingest" / Segment) { ingestor =>
          headerValueByName(IngestionParams.HYDRA_REQUEST_LABEL_PARAM) { destination =>
            onComplete(lookupIngestor(ingestor)) {
              case Success(result) =>
                result.ref match {
                  case Some(ref) =>
                    entity(as[String]) { payload =>
                      imperativelyComplete { ictx =>
                        val hydraReq = buildRequest(destination, ingestor, payload, ictx.ctx)
                        ingestorRegistry.foreach { registry =>
                          actorRefFactory.actorOf(IngestionRequestHandler.props(hydraReq, registry, ictx))
                        }
                      }
                    }
                  case None => complete(404, GenericServiceResponse(404, s"Ingestor $ingestor not found."))
                }

              case Failure(ex) => throw ex
            }
          }
        }
      }
    }


  //todo: add retry strategy, etc. stuff
  private def buildRequest(destination: String, ingestor: String, payload: String, ctx: RequestContext) = {
    val metadata: List[HydraRequestMedatata] = List(ctx.request.headers.map(header =>
      HydraRequestMedatata(header.name.toLowerCase, header.value)): _*)
    HydraRequest(destination, payload, metadata).withMetadata(IngestionParams.HYDRA_INGESTOR_TARGET_PARAM -> ingestor)
  }

  val excptHandler = ExceptionHandler {
    case e: IllegalArgumentException => complete(GenericServiceResponse(400, e.getMessage))
    case e: Exception => complete(GenericServiceResponse(ServiceUnavailable.intValue, e.getMessage))
  }
}
