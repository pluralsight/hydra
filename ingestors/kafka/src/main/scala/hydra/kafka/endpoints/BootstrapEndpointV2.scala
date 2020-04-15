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
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import hydra.common.util.Futurable
import hydra.core.http.CorsSupport
import hydra.kafka.algebras.MetadataAlgebra
import hydra.kafka.model.{TopicMetadataV2Adapter, TopicMetadataV2Request}
import hydra.kafka.programs.CreateTopicProgram
import hydra.kafka.serializers.TopicMetadataV2Parser
import hydra.kafka.util.KafkaUtils.TopicDetails

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

final class BootstrapEndpointV2[F[_]: Futurable](
    createTopicProgram: CreateTopicProgram[F],
    defaultTopicDetails: TopicDetails,
    metadataAlgebra: MetadataAlgebra[F]
) extends CorsSupport with TopicMetadataV2Adapter {

  import TopicMetadataV2Parser._

  val route: Route = cors(settings) {
    pathPrefix("v2" / "streams") {
      post {
        pathEndOrSingleSlash {
          entity(as[TopicMetadataV2Request]) { t =>
            onComplete(
              Futurable[F].unsafeToFuture(createTopicProgram
                .createTopic(t, defaultTopicDetails))
            ) {
              case Success(_) => complete(StatusCodes.OK)
              case Failure(e) => complete(StatusCodes.InternalServerError, e)
            }
          }
        }
      } ~
      get {
        pathEndOrSingleSlash {
          onComplete(Futurable[F].unsafeToFuture(metadataAlgebra.getAllMetadata)) {
            case Success(metadata) => complete(StatusCodes.OK, metadata.map(toResource))
            case Failure(e) => complete(StatusCodes.InternalServerError, e)
          }
        }
      }
    }
  }

}
