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

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.pattern.ask
import akka.util.Timeout
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import hydra.avro.resource.SchemaResource
import hydra.common.config.ConfigSupport
import hydra.core.akka.SchemaRegistryActor
import hydra.core.akka.SchemaRegistryActor._
import hydra.core.http.{CorsSupport, RouteSupport}
import hydra.core.marshallers.GenericServiceResponse
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.apache.avro.SchemaParseException
import spray.json.RootJsonFormat

import scala.concurrent.duration._

/**
  * A wrapper around Confluent's schema registry that facilitates schema registration and retrieval.
  *
  * Created by alexsilva on 2/13/16.
  */
class SchemasEndpoint()(implicit system: ActorSystem)
    extends RouteSupport
    with ConfigSupport
    with CorsSupport {

  implicit val endpointFormat: RootJsonFormat[SchemasEndpointResponse] = jsonFormat3(SchemasEndpointResponse.apply)
  implicit val v2EndpointFormat: RootJsonFormat[SchemasWithKeyEndpointResponse] = jsonFormat2(SchemasWithKeyEndpointResponse.apply)
  implicit val timeout: Timeout = Timeout(3.seconds)

  private val schemaRegistryActor =
    system.actorOf(SchemaRegistryActor.props(applicationConfig))

  override def route: Route = cors(settings) {
    handleExceptions(excptHandler) {
      pathPrefix("schemas") {
        get {
          pathEndOrSingleSlash {
            onSuccess(
              (schemaRegistryActor ? FetchSubjectsRequest)
                .mapTo[FetchSubjectsResponse]
            ) { response => complete(OK, response.subjects) }
          } ~ path(Segment) { subject =>
            parameters('schema ?) { schemaOnly: Option[String] =>
              getSchema(includeKeySchema = false, subject, schemaOnly)
            }
          } ~ path(Segment / "versions") { subject =>
            onSuccess(
              (schemaRegistryActor ? FetchAllSchemaVersionsRequest(subject))
                .mapTo[FetchAllSchemaVersionsResponse]
            ) { response =>
              complete(OK, response.versions.map(SchemasEndpointResponse(_)))
            }
          } ~ path(Segment / "versions" / IntNumber) { (subject, version) =>
            onSuccess(
              (schemaRegistryActor ? FetchSchemaVersionRequest(
                subject,
                version
              )).mapTo[FetchSchemaVersionResponse]
            ) { response =>
              complete(OK, SchemasEndpointResponse(response.schemaResource))
            }
          }
        } ~
          post {
            registerNewSchema
          }
      } ~ v2Route
    }
  }

  private val v2Route =
    pathPrefix("v2") {
      get {
        path("schemas" / Segment) { subject =>
          getSchema(includeKeySchema = true, subject, None)
        }
      }
    }

  def getSchema(includeKeySchema: Boolean, subject: String, schemaOnly: Option[String]): Route = {
    onSuccess(
      (schemaRegistryActor ? FetchSchemaRequest(subject))
        .mapTo[FetchSchemaResponse]
    ) { response =>

      if (includeKeySchema) {
        complete(OK, SchemasWithKeyEndpointResponse.apply(response))
      } else {
        val schemaResource = response.schemaResource
        schemaOnly.map(_ => complete(OK, schemaResource.schema.toString))
          .getOrElse(
            complete(OK, SchemasEndpointResponse(schemaResource))
          )
      }
    }
  }

  private def registerNewSchema: Route = {
    entity(as[String]) { json =>
      extractRequest { request =>
        onSuccess(
          (schemaRegistryActor ? RegisterSchemaRequest(json))
            .mapTo[RegisterSchemaResponse]
        ) { registeredSchema =>
          respondWithHeader(
            Location(
              request.uri.copy(path =
                request.uri.path / registeredSchema.schemaResource.schema.getFullName
              )
            )
          ) {
            complete(Created, SchemasEndpointResponse(registeredSchema))
          }
        }
      }
    }
  }

  private[http] val excptHandler: ExceptionHandler = ExceptionHandler {
    case e: RestClientException if e.getErrorCode == 40401 =>
      complete(NotFound, GenericServiceResponse(404, e.getMessage))

    case e: RestClientException =>
      val registryHttpStatus = e.getStatus
      val registryErrorCode = e.getErrorCode
      complete(
        registryHttpStatus,
        GenericServiceResponse(
          registryErrorCode,
          s"Registry error: ${e.getMessage}"
        )
      )

    case e: SchemaParseException =>
      complete(
        BadRequest,
        GenericServiceResponse(
          400,
          s"Unable to parse avro schema: ${e.getMessage}"
        )
      )

    case e: Exception =>
      extractUri { uri =>
        log.warn(s"Request to $uri failed with exception: {}", e)
        complete(
          BadRequest,
          GenericServiceResponse(
            400,
            s"Unable to complete request for ${uri.path.tail} : ${e.getMessage}"
          )
        )
      }
  }
}

case class SchemasWithKeyEndpointResponse(keySchemaResponse: Option[SchemasEndpointResponse], valueSchemaResponse: SchemasEndpointResponse)

object SchemasWithKeyEndpointResponse {
  def apply(f: FetchSchemaResponse): SchemasWithKeyEndpointResponse =
    new SchemasWithKeyEndpointResponse(
      f.keySchemaResource.map(SchemasEndpointResponse.apply),
      SchemasEndpointResponse.apply(f.schemaResource)
    )
}

case class SchemasEndpointResponse(id: Int, version: Int, schema: String)

object SchemasEndpointResponse {

  def apply(resource: SchemaResource): SchemasEndpointResponse =
    SchemasEndpointResponse(
      resource.id,
      resource.version,
      resource.schema.toString
    )

  def apply(
      registeredSchema: SchemaRegistryActor.RegisterSchemaResponse
  ): SchemasEndpointResponse = {
    val resource = registeredSchema.schemaResource
    SchemasEndpointResponse(
      resource.id,
      resource.version,
      resource.schema.toString
    )
  }
}
