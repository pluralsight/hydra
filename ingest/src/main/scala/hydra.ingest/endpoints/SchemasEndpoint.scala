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
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.core.avro.registry.ConfluentSchemaRegistry
import hydra.core.marshallers.{GenericServiceResponse, HydraJsonSupport}
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
    with ConfluentSchemaRegistry {

  implicit val endpointFormat = jsonFormat3(SchemasEndpointResponse)

  override def route: Route =
    get {
      pathPrefix("schemas" / Segment) { subject =>
        parameters('pretty ? "false", 'version.as[Int].?) { (pretty, version) =>
          handleExceptions(excptHandler) {
            val schemaSubject = subject + "-value"
            val schemaMeta = version.map(v => registry.getSchemaMetadata(subject, v))
              .getOrElse(registry.getLatestSchemaMetadata(schemaSubject))
            val schema = registry.getByID(schemaMeta.getId)
            val txt = schema.toString(pretty.toBoolean)
            complete(OK, txt)
          }
        }
      }
    } ~
      post {
        pathPrefix("schemas") {
          entity(as[String]) { json =>
            handleExceptions(excptHandler) {
              extractRequest { request =>
                val schema = new Parser().parse(json)
                val name = schema.getNamespace() + "." + schema.getName()
                log.debug(s"Registering schema $name: $json")
                val id = registry.register(name + "-value", schema)
                respondWithHeader(Location(request.uri.copy(path = request.uri.path / name))) {
                  complete(Created, SchemasEndpointResponse(201, "Schema registered.", id.toString))
                }
              }
            }
          }
        }
      }


  val excptHandler = ExceptionHandler {
    case e: RestClientException =>
      extractUri { uri =>
        complete(BadRequest, GenericServiceResponse(e.getErrorCode, s"Registry error: ${e.getMessage}"))
      }

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

case class SchemasEndpointResponse(status: Int, description: String, id: String)