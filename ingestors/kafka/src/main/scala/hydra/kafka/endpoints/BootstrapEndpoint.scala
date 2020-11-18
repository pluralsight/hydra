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
import akka.http.scaladsl.server.Directives.{complete, extractExecutionContext}
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.pattern.ask
import akka.util.Timeout
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import hydra.common.logging.LoggingAdapter
import hydra.core.http.{CorsSupport, HydraDirectives, RouteSupport}
import hydra.core.marshallers.TopicMetadataRequest
import hydra.core.monitor.HydraMetrics.addPromHttpMetric
import hydra.kafka.model.TopicMetadataAdapter
import hydra.kafka.services.TopicBootstrapActor._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class BootstrapEndpoint(override val system:ActorSystem) extends RouteSupport
  with LoggingAdapter
  with TopicMetadataAdapter
  with HydraDirectives
  with CorsSupport
  with BootstrapEndpointActors {

  private implicit val timeout = Timeout(10.seconds)

  override val route: Route = cors(settings) {
    handleExceptions(exceptionHandler("Bootstrap")) {
      extractExecutionContext { implicit ec =>
        pathPrefix("streams") {
          pathEndOrSingleSlash {
            post {
              requestEntityPresent {
                entity(as[TopicMetadataRequest]) { topicMetadataRequest =>
                  val topic = topicMetadataRequest.schema.getFields("namespace", "name").mkString(".").replaceAll("\"","")
                  onComplete(
                    bootstrapActor ? InitiateTopicBootstrap(topicMetadataRequest)
                  ) {

                    case Success(message) =>
                      message match {

                        case BootstrapSuccess(metadata) =>
                          addPromHttpMetric(topic, StatusCodes.OK.toString, "Bootstrap")
                          complete(StatusCodes.OK, toResource(metadata))

                        case BootstrapFailure(reasons) =>
                          addPromHttpMetric(topic, StatusCodes.BadRequest.toString, "Bootstrap")
                          complete(StatusCodes.BadRequest, reasons)

                        case e: Exception =>
                          log.error("Unexpected error in TopicBootstrapActor", e)
                          addPromHttpMetric(topic, StatusCodes.InternalServerError.toString, "Bootstrap")
                          complete(StatusCodes.InternalServerError, e.getMessage)
                      }

                    case Failure(ex) =>
                      log.error("Unexpected error in BootstrapEndpoint", ex)
                      addPromHttpMetric(topic, StatusCodes.InternalServerError.toString, "Bootstrap")
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
    }
  }

  private def getAllStreams(subject: Option[String]): Route = {
    extractExecutionContext { implicit ec =>
      onSuccess(bootstrapActor ? GetStreams(subject)) {
        case GetStreamsResponse(metadata) =>
          addPromHttpMetric("", StatusCodes.OK.toString, "getAllStreams")
          complete(StatusCodes.OK, metadata.map(toResource))
        case Failure(ex) =>
          throw ex
        case x =>
          log.error("Unexpected error in BootstrapEndpoint", x)
          addPromHttpMetric("", StatusCodes.InternalServerError.toString, "getAllStreams")
          complete(StatusCodes.InternalServerError, "Unknown error")
      }
    }
  }

  private def exceptionHandler(topic: String) = ExceptionHandler {
    case e =>
      extractExecutionContext { implicit ec =>
        addPromHttpMetric(topic, StatusCodes.InternalServerError.toString,"Bootstrap")
        complete(500, e.getMessage)
      }
  }
}