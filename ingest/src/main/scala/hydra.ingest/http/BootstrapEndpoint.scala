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
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import configs.syntax._
import hydra.common.logging.LoggingAdapter
import hydra.core.akka.SchemaRegistryActor
import hydra.core.http.HydraDirectives
import hydra.core.marshallers.{GenericError, HydraJsonSupport, TopicMetadataRequest}
import hydra.ingest.bootstrap.HydraIngestorRegistryClient
import hydra.ingest.services.TopicBootstrapActor.{ActorInitializing, BootstrapFailure, BootstrapSuccess, InitiateTopicBootstrap}
import hydra.ingest.services._
import spray.json.DeserializationException

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}


class BootstrapEndpoint(implicit val system: ActorSystem, implicit val e: ExecutionContext)
  extends RoutedEndpoints with LoggingAdapter with HydraJsonSupport with HydraDirectives {

  implicit val timeout = Timeout(10.seconds)

  implicit val mat = ActorMaterializer()

  private val registryPath = HydraIngestorRegistryClient.registryPath(applicationConfig)

  private val kafkaIngestor = system.actorSelection(s"$registryPath/kafka_ingestor")

  private val schemaRegistryActor = system.actorOf(SchemaRegistryActor.props(applicationConfig))

  private val bootstrapActor = system.actorOf(
    TopicBootstrapActor.props(applicationConfig, schemaRegistryActor, kafkaIngestor))

  private val ingestTimeout = applicationConfig.get[FiniteDuration]("ingest.timeout")
    .valueOrElse(500 millis)

  override val route: Route =
    pathPrefix("topics") {
      pathEndOrSingleSlash {
        handleExceptions(exceptionHandler) {
          post {
            requestEntityPresent {
              entity(as[TopicMetadataRequest]) { topicMetadataRequest =>
                onSuccess(bootstrapActor ? InitiateTopicBootstrap(topicMetadataRequest)) {
                  case BootstrapSuccess => complete(StatusCodes.OK)
                  case BootstrapFailure(reasons) => complete(StatusCodes.BadRequest, reasons)
                  case ActorInitializing => complete(StatusCodes.InternalServerError, "Please try again later....")
                  case e: Exception =>
                    complete(StatusCodes.InternalServerError, e.getMessage)
                }
              }
            }
          }
        }
      }
    }

  private def exceptionHandler = ExceptionHandler {
    case e: DeserializationException =>
      log.error(s"Payload failed deserialization, please check metadata structure")
      complete(400, e)

    case t: Throwable =>
      log.error(s"Encountered $t while handling request in bootstrap endpoint...")
      complete(400, GenericError(400, t.getMessage))
  }
}