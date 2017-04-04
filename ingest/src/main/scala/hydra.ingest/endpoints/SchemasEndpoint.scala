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

package hydra.ingest.endpoints

import akka.actor.{ActorRefFactory, ActorSystem}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import ch.megard.akka.http.cors.CorsDirectives._
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.core.avro.registry.ConfluentSchemaRegistry
import hydra.core.http.CorsSupport
import hydra.core.marshallers.{GenericServiceResponse, HydraJsonSupport}
import io.confluent.kafka.schemaregistry.client.SchemaMetadata
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.apache.avro.Schema.Parser
import org.apache.avro.SchemaParseException


/**
  * A wrapper around Confluent's schema registry that facilitates schema registration and retrieval.
  *
  * Created by alexsilva on 2/13/16.
  */
class SchemasEndpoint(implicit system: ActorSystem, implicit val actorRefFactory: ActorRefFactory)
  extends RoutedEndpoints with ConfigSupport with LoggingAdapter with HydraJsonSupport
    with ConfluentSchemaRegistry with CorsSupport {

  implicit val endpointFormat = jsonFormat3(SchemasEndpointResponse.apply)

  implicit val ec = system.dispatcher

  override def route: Route = cors(settings) {
    pathPrefix("schemas") {
      handleExceptions(excptHandler) {
        get {
          pathEndOrSingleSlash {
            onSuccess(getAllSubjects) { subjects =>
              complete(OK, subjects)
            }
          } ~ path(Segment) { subject =>
            parameters('schema ?) { schemaOnly =>
              val meta = registry.getLatestSchemaMetadata(subject + "-value")
              schemaOnly.map(_ => complete(OK, meta.getSchema)).getOrElse(complete(OK, SchemasEndpointResponse(meta)))
            }
          } ~ path(Segment / "versions") { subject =>
            val schemaMeta = registry.getLatestSchemaMetadata(subject + "-value")
            val v = schemaMeta.getVersion
            val versions = (1 to v) map (vs => SchemasEndpointResponse(registry.getSchemaMetadata(subject + "-value", vs)))
            complete(OK, versions)
          } ~ path(Segment / "versions" / IntNumber) { (subject, version) =>
            val meta = registry.getSchemaMetadata(subject + "-value", version)
            complete(OK, SchemasEndpointResponse(meta.getId, meta.getVersion, Some(meta.getSchema)))
          }
        } ~
          post {
            entity(as[String]) { json =>
              extractRequest { request =>
                val schema = new Parser().parse(json)
                val name = schema.getNamespace() + "." + schema.getName()
                log.debug(s"Registering schema $name: $json")
                val id = registry.register(name + "-value", schema)
                respondWithHeader(Location(request.uri.copy(path = request.uri.path / name))) {
                  complete(Created, SchemasEndpointResponse(id, 1, Some(json)))
                }
              }
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
        complete(BadRequest,
          GenericServiceResponse(400, s"Unable to complete request for ${uri.path.tail} : ${e.getMessage}"))
      }
  }
}

case class SchemasEndpointResponse(id: Int, version: Int, schema: Option[String] = None)

object SchemasEndpointResponse {
  def apply(meta: SchemaMetadata): SchemasEndpointResponse =
    SchemasEndpointResponse(meta.getId, meta.getVersion, Some(meta.getSchema))
}