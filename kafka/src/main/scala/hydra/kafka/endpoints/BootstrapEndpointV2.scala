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
import akka.http.scaladsl.server.Route
import cats.effect.{IO, Timer}
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import com.pluralsight.hydra.reflect.DoNotScan
import com.typesafe.config.ConfigFactory
import hydra.avro.registry.SchemaRegistry
import hydra.core.bootstrap.CreateTopicProgram
import hydra.core.http.CorsSupport
import hydra.kafka.model.{Subject, TopicMetadataV2Request}
import hydra.kafka.serializers.TopicMetadataV2Parser
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.avro.Schema
import retry.{RetryPolicies, RetryPolicy}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

// This trait is the worst, but can't be removed until we remove service-container :(
private[endpoints] trait CreateTopicDependency {

  private implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)
  private implicit val logger: Logger[IO] = Slf4jLogger.getLogger

  private val schemaRegistry = SchemaRegistry.live[IO](ConfigFactory.load().getString("hydra.schema.registry.url"), 100).unsafeRunSync()

  def createTopic(subject: Subject, key: Schema, value: Schema): IO[Unit] = {
    val retryPolicy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
    new CreateTopicProgram[IO](schemaRegistry, retryPolicy).createTopic(subject.value, key, value)
  }
}

class BootstrapEndpointV2(implicit val system: ActorSystem, implicit val e: ExecutionContext)
  extends RoutedEndpoints with CorsSupport with CreateTopicDependency {

  import TopicMetadataV2Parser._

  override val route: Route = cors(settings) {
    path("v2" / "streams") {
      post {
        pathEndOrSingleSlash {
          entity(as[TopicMetadataV2Request]) { t =>
            onComplete(createTopic(t.subject, t.schemas.key, t.schemas.value).unsafeToFuture()) {
              case Success(_) => complete(StatusCodes.OK)
              case Failure(e) => complete(StatusCodes.InternalServerError, e)
            }
          }
        }
      }
    }
  }

}
