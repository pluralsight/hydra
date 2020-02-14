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

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import hydra.common.logging.LoggingAdapter
import hydra.core.http.{CorsSupport, HydraDirectives}
import hydra.kafka.model.{TopicMetadataAdapter, TopicMetadataV2Request}
import hydra.kafka.serializers.TopicMetadataV2Parser

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._


class TopicEndpoint (implicit val system: ActorSystem, implicit val e: ExecutionContext)
  extends RoutedEndpoints
    with LoggingAdapter
    with TopicMetadataAdapter
    with HydraDirectives
    with CorsSupport
    with BootstrapEndpointActors
    with TopicMetadataV2Parser {

  private implicit val timeout = Timeout(10.seconds)

  override val route: Route = cors(settings) {
    pathPrefix("/v2/streams") {
      pathEndOrSingleSlash {
        post {
          requestEntityPresent {
            entity(as[TopicMetadataV2Request]) {
              topicMetadataV2Request =>
            }
          }
        }
      }
    }
  }

}
