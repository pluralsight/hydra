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

package hydra.kafka.endpoints

import akka.actor._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import hydra.common.logging.LoggingAdapter
import hydra.core.akka.SchemaRegistryActor
import hydra.core.http.HydraDirectives
import hydra.core.marshallers.{HydraJsonSupport, TopicMetadataRequest}
import hydra.kafka.services.TopicBootstrapActor
import hydra.kafka.services.TopicBootstrapActor.{BootstrapFailure, BootstrapSuccess, InitiateTopicBootstrap}
import org.apache.commons.lang3.ClassUtils

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success}


class BootstrapEndpoint(implicit val system: ActorSystem, implicit val e: ExecutionContext)
  extends RoutedEndpoints with LoggingAdapter with HydraJsonSupport with HydraDirectives {

  private implicit val timeout = Timeout(10.seconds)

  private implicit val mat = ActorMaterializer()


  private val kafkaIngestor = system.actorSelection("/user/service/ingestor_registry/kafka_ingestor")

  private val schemaRegistryActor = system.actorOf(SchemaRegistryActor.props(applicationConfig), name = ClassUtils.getSimpleName(SchemaRegistryActor.getClass))

  private val bootstrapActor = system.actorOf(
    TopicBootstrapActor.props(applicationConfig, schemaRegistryActor, kafkaIngestor))

  override val route: Route =
    pathPrefix("topics") {
      pathEndOrSingleSlash {
        post {
          requestEntityPresent {
            entity(as[TopicMetadataRequest]) { topicMetadataRequest =>
              onComplete(bootstrapActor ? InitiateTopicBootstrap(topicMetadataRequest)) {
                case Success(message) => message match {

                  case BootstrapSuccess =>
                    complete(StatusCodes.OK)

                  case BootstrapFailure(reasons) =>
                    complete(StatusCodes.BadRequest, reasons)

                  case e: Exception =>
                    log.error("Unexpected error in TopicBootstrapActor", e)
                    complete(StatusCodes.InternalServerError, e.getMessage)
                }

                case Failure(ex) =>
                  log.error("Unexpected error in BootstrapEndpoint", ex)
                  complete(StatusCodes.InternalServerError, ex.getMessage)
              }
            }
          }
        }
      }
    }
}
