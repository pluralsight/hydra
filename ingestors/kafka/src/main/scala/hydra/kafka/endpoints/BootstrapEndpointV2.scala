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

import java.time.Instant

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import hydra.avro.registry.SchemaRegistry.IncompatibleSchemaException
import hydra.common.util.Futurable
import hydra.core.http.CorsSupport
import hydra.core.marshallers.GenericError
import hydra.core.monitor.HydraMetrics.addHttpMetric
import hydra.kafka.algebras.TagsAlgebra
import hydra.kafka.model.TopicMetadataV2Request
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.programs.CreateTopicProgram
import hydra.kafka.serializers.TopicMetadataV2Parser
import hydra.kafka.util.KafkaUtils.TopicDetails

import scala.util.{Failure, Success}

final class BootstrapEndpointV2[F[_]: Futurable](
    createTopicProgram: CreateTopicProgram[F],
    defaultTopicDetails: TopicDetails,
    tagsAlgebra: TagsAlgebra[F]
) extends CorsSupport {

  import TopicMetadataV2Parser._

  val route: Route = cors(settings) {
    extractExecutionContext { implicit ec =>
      extractMethod { method =>
        pathPrefix("v2" / "topics" / Segment) { topicName =>
          val startTime = Instant.now
          handleExceptions(exceptionHandler(topicName, startTime, method.value)) {
            pathEndOrSingleSlash {
              put {
                entity(as[TopicMetadataV2Request]) { p =>
                  val t = p.copy(tags = if(p.tags.contains("DVS")) p.tags else p.tags ++ List("DVS")) //adding "DVS" tag
                  onComplete(Futurable[F].unsafeToFuture(tagsAlgebra.validateTags(t.tags))) {
                    case Failure(exception) =>
                      addHttpMetric(topicName, StatusCodes.BadRequest, "V2Bootstrap", startTime, method.value, error = Some(exception.getMessage))
                      complete(StatusCodes.BadRequest, exception.getMessage)
                    case Success(_) =>
                        Subject.createValidated(topicName) match {
                          case Some(validatedTopic) =>
                            onComplete(
                              Futurable[F].unsafeToFuture(createTopicProgram
                                .createTopic(validatedTopic, t, defaultTopicDetails))
                            ) {
                              case Success(_) =>
                                addHttpMetric(topicName, StatusCodes.OK, "V2Bootstrap", startTime, "PUT")
                                complete(StatusCodes.OK)
                              case Failure(IncompatibleSchemaException(m)) =>
                                addHttpMetric(topicName, StatusCodes.BadRequest, "V2Bootstrap", startTime, "PUT", error = Some(s"$IncompatibleSchemaException"))
                                complete(StatusCodes.BadRequest, m)
                              case Failure(e) =>
                                addHttpMetric(topicName, StatusCodes.InternalServerError, "V2Bootstrap", startTime, "PUT", error = Some(e.getMessage))
                                complete(StatusCodes.InternalServerError, e)
                            }
                          case None =>
                            addHttpMetric(topicName, StatusCodes.BadRequest, "V2Bootstrap", startTime, "PUT", error = Some(Subject.invalidFormat))
                            complete(StatusCodes.BadRequest, Subject.invalidFormat)
                      }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  private def exceptionHandler(topic: String, startTime: Instant, method: String) = ExceptionHandler {
    case e =>
      extractExecutionContext { implicit ec =>
        addHttpMetric(topic, StatusCodes.InternalServerError,"V2Bootstrap", startTime, method, error = Some(e.getMessage))
        complete(500, e.getMessage)
      }
  }

}
