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
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import hydra.avro.registry.ConfluentSchemaRegistry
import hydra.common.logging.LoggingAdapter
import hydra.core.akka.SchemaRegistryActor
import hydra.core.http.{CorsSupport, HydraDirectives}
import hydra.core.marshallers.TopicMetadataRequest
import hydra.kafka.model.TopicMetadataAdapter
import hydra.kafka.services.TopicBootstrapActor._
import hydra.kafka.services.{StreamsManagerActor, TopicBootstrapActor}
import hydra.kafka.util.KafkaUtils

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success}


class BootstrapEndpoint(implicit val system: ActorSystem, implicit val e: ExecutionContext)
  extends RoutedEndpoints
    with LoggingAdapter
    with TopicMetadataAdapter
    with HydraDirectives
    with CorsSupport {

  private implicit val timeout = Timeout(10.seconds)

  private implicit val mat = ActorMaterializer()

  private val kafkaIngestor = system.actorSelection(
    path = applicationConfig.getString("kafka-ingestor-path"))

  private val schemaRegistryActor = system.actorOf(SchemaRegistryActor.props(applicationConfig))

  private val bootstrapKafkaConfig = applicationConfig.getConfig("bootstrap-config")

  private val streamsManagerProps = StreamsManagerActor.props(bootstrapKafkaConfig,
    KafkaUtils.BootstrapServers, ConfluentSchemaRegistry.forConfig(applicationConfig).registryClient)


  private val bootstrapActor = system.actorOf(
    TopicBootstrapActor.props(schemaRegistryActor, kafkaIngestor, streamsManagerProps, Some(bootstrapKafkaConfig)))


  override val route: Route = cors(settings) {
    pathPrefix("streams" | "topics") {
      pathEndOrSingleSlash {
        post {
          requestEntityPresent {
            entity(as[TopicMetadataRequest]) { topicMetadataRequest =>
              onComplete(bootstrapActor ? InitiateTopicBootstrap(topicMetadataRequest)) {
                case Success(message) => message match {

                  case BootstrapSuccess(metadata) =>
                    complete(StatusCodes.OK, toResource(metadata))

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
      } ~ get {
        pathEndOrSingleSlash(getAllStreams(None)) ~
          path(Segment)(subject => getAllStreams(Some(subject)))
      }
    }
  }

  private def getAllStreams(subject: Option[String]): Route = {
    onSuccess(bootstrapActor ? GetStreams(subject)) {
      case GetStreamsResponse(metadata) =>
        complete(StatusCodes.OK, metadata.map(toResource))
      case Failure(ex) =>
        throw ex
      case x =>
        log.error("Unexpected error in BootstrapEndpoint", x)
        complete(StatusCodes.InternalServerError, "Unknown error")
    }
  }
}
