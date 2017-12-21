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
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.StatusCodes.ServiceUnavailable
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.stream.ActorMaterializer
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import hydra.common.logging.LoggingAdapter
import hydra.core.http.HydraDirectives
import hydra.core.ingest.RequestParams
import hydra.core.marshallers.{GenericServiceResponse, HydraJsonSupport}
import hydra.ingest.bootstrap.HydraIngestorRegistry
import hydra.ingest.services.IngestionRequestHandler

import scala.concurrent.ExecutionContext
import scala.util.Random

/**
  * Created by alexsilva on 12/22/15.
  */
class IngestionEndpoint(implicit val system: ActorSystem, implicit val e: ExecutionContext)
  extends RoutedEndpoints with LoggingAdapter with HydraJsonSupport with HydraDirectives with HydraIngestorRegistry {

  import hydra.ingest.bootstrap.RequestFactories._

  implicit val mat = ActorMaterializer()

  override val route: Route =
    post {
      requestEntityPresent {
        pathPrefix("ingest") {
          // decodeRequestWith(Gzip, Deflate) {
          parameter("correlationId".as[Long] ?) { correlationId =>
            handleExceptions(excptHandler) {
              pathEndOrSingleSlash {
                broadcastRequest(correlationId.getOrElse(randomId))
              }
            } ~ path(Segment) { ingestor =>
              publishToIngestor(correlationId.getOrElse(randomId), ingestor)
            }
          }
        }
        //}
      }
    }

  val excptHandler = ExceptionHandler {
    case e: IllegalArgumentException => complete(GenericServiceResponse(400, e.getMessage))
    case e: Exception => complete(GenericServiceResponse(ServiceUnavailable.intValue, e.getMessage))
  }

  private def randomId = Random.nextLong()

  def broadcastRequest(correlationId: Long) = {
    onSuccess(ingestorRegistry) { registry =>
      extractRequestContext { ctx =>
        val hydraReq = createRequest[HttpRequest](correlationId, ctx.request)
        onSuccess(hydraReq) { request =>
          imperativelyComplete { ictx =>
            system.actorOf(IngestionRequestHandler.props(request, registry, ictx))
          }
        }
      }
    }
  }

  def publishToIngestor(correlationId: Long, ingestor: String) = {
    onSuccess(lookupIngestor(ingestor)) { result =>
      result.ingestors.headOption match {
        case Some(_) =>
          extractRequest { httpRequest =>
            val hydraReqFuture = createRequest[HttpRequest](correlationId, httpRequest)
            onSuccess(hydraReqFuture) { r =>
              val hydraReq = r.withMetadata(RequestParams.HYDRA_INGESTOR_PARAM -> ingestor)
              imperativelyComplete { ictx =>
                ingestorRegistry.foreach(registry =>
                  system.actorOf(IngestionRequestHandler.props(hydraReq, registry, ictx)))
              }
            }
          }
        case None => complete(404, GenericServiceResponse(404, s"Ingestor $ingestor not found."))
      }
    }
  }
}