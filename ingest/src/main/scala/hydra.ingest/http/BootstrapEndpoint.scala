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
import akka.http.scaladsl.model.StatusCodes.BadRequest
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.stream.ActorMaterializer
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import configs.syntax._
import hydra.common.logging.LoggingAdapter
import hydra.core.akka.SchemaRegistryActor
import hydra.core.http.HydraDirectives
import hydra.core.marshallers.{GenericError, HydraJsonSupport, TopicMetadataRequest}
import hydra.ingest.bootstrap.HydraIngestorRegistryClient
import hydra.ingest.services.TopicBootstrapActor.{InitiateTopicBootstrap}
import hydra.ingest.services._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}


class BootstrapEndpoint(implicit val system: ActorSystem, implicit val e: ExecutionContext)
  extends RoutedEndpoints with LoggingAdapter with HydraJsonSupport with HydraDirectives {

  implicit val mat = ActorMaterializer()

  //for performance reasons, we give this endpoint its own instance of the gateway
  private val registryPath = HydraIngestorRegistryClient.registryPath(applicationConfig)

  private val requestHandler = system.actorOf(IngestionHandlerGateway.props(registryPath),
    "ingestion_Http_handler_gateway")

  private val schemaRegistryActor = system.actorOf(SchemaRegistryActor.props(applicationConfig))

  private val bootstrapActor = system.actorOf(TopicBootstrapActor.props(applicationConfig, schemaRegistryActor, requestHandler))

  private val ingestTimeout = applicationConfig.get[FiniteDuration]("ingest.timeout")
    .valueOrElse(500 millis)

  override val route: Route =
    pathPrefix("topics") {
      pathEndOrSingleSlash {
        post {
          requestEntityPresent {
              entity(as[TopicMetadataRequest]) {
                topicMetadataRequest =>
                  imperativelyComplete {
                    ctx =>
                      bootstrapActor ! InitiateTopicBootstrap(topicMetadataRequest, ctx)
                  }
              }
          }
        }
      }
    } ~ complete(BadRequest, "This endpoint requires a payload.")
}