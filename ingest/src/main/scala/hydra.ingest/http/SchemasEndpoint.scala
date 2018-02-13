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

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.{ ExceptionHandler, Route }
import akka.pattern.ask
import akka.util.Timeout
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.core.akka.SchemaRegistryActor
import hydra.core.akka.SchemaRegistryActor._
import hydra.core.http.CorsSupport
import hydra.core.marshallers.{ GenericServiceResponse, HydraJsonSupport }
import io.confluent.kafka.schemaregistry.client.SchemaMetadata
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.apache.avro.SchemaParseException

/**
 * A wrapper around Confluent's schema registry that facilitates schema registration and retrieval.
 *
 * Created by alexsilva on 2/13/16.
 */
class SchemasEndpoint(implicit system: ActorSystem, implicit val e: ExecutionContext)
  extends RoutedEndpoints with ConfigSupport with LoggingAdapter with HydraJsonSupport with CorsSupport {

  implicit val endpointFormat = jsonFormat3(SchemasEndpointResponse.apply)
  implicit val timeout = Timeout(3.seconds)

  private val schemaRegistryActor = system.actorOf(SchemaRegistryActor.props(applicationConfig))

  override def route: Route = cors(settings) {
    pathPrefix("schemas") {
      handleExceptions(excptHandler) {
        get {
          pathEndOrSingleSlash {
            onSuccess((schemaRegistryActor ? FetchSubjectsRequest).mapTo[FetchSubjectsResponse]) { response =>
              complete(OK, response.subjects)
            }
          } ~ path(Segment) { subject =>
            parameters('schema ?) { schemaOnly: Option[String] =>
              //TODO: make field selection more generic, i.e. /<subject>?fields=schema
              onSuccess((schemaRegistryActor ? FetchSchemaMetadataRequest(subject)).mapTo[FetchSchemaMetadataResponse]) { response =>
                val metadata = response.schemaMetadata
                schemaOnly.map(_ => complete(OK, metadata.getSchema))
                  .getOrElse(complete(OK, SchemasEndpointResponse(metadata)))
              }
            }
          } ~ path(Segment / "versions") { subject =>
            onSuccess((schemaRegistryActor ? FetchAllSchemaVersionsRequest(subject)).mapTo[FetchAllSchemaVersionsResponse]) { response =>
              complete(OK, response.versions.map(SchemasEndpointResponse(_)))
            }
          } ~ path(Segment / "versions" / IntNumber) { (subject, version) =>
            onSuccess((schemaRegistryActor ? FetchSchemaVersionRequest(subject, version)).mapTo[FetchSchemaVersionResponse]) { response =>
              complete(OK, SchemasEndpointResponse(response.schemaMetadata))
            }
          }
        } ~
          post { registerNewSchema }
      }
    }
  }

  def registerNewSchema = {
    entity(as[String]) { json =>
      extractRequest { request =>
        onSuccess((schemaRegistryActor ? RegisterSchemaRequest(json)).mapTo[RegisterSchemaResponse]) { registeredSchema =>
          respondWithHeader(Location(request.uri.copy(path = request.uri.path / registeredSchema.name))) {
            complete(Created, SchemasEndpointResponse(registeredSchema))
          }
        }
      }
    }
  }

  val excptHandler = ExceptionHandler {
    case e: RestClientException if (e.getErrorCode == 40401) =>
      complete(NotFound, GenericServiceResponse(404, e.getMessage))

    case e: RestClientException =>
      complete(BadRequest, GenericServiceResponse(e.getErrorCode, s"Registry error: ${e.getMessage}"))

    case e: SchemaParseException =>
      complete(BadRequest, GenericServiceResponse(400, s"Unable to parse avro schema: ${e.getMessage}"))

    case e: Exception =>
      extractUri { uri =>
        log.warn(s"Request to $uri could not be handled normally")
        complete(
          BadRequest,
          GenericServiceResponse(400, s"Unable to complete request for ${uri.path.tail} : ${e.getMessage}"))
      }
  }
}

case class SchemasEndpointResponse(id: Int, version: Int, schema: String)

object SchemasEndpointResponse {
  def apply(meta: SchemaMetadata): SchemasEndpointResponse =
    SchemasEndpointResponse(meta.getId, meta.getVersion, meta.getSchema)
  def apply(registeredSchema: SchemaRegistryActor.RegisterSchemaResponse): SchemasEndpointResponse =
    SchemasEndpointResponse(registeredSchema.id, registeredSchema.version, registeredSchema.schema)
}
