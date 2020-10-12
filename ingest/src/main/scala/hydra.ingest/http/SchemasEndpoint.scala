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

import akka.actor.{ActorSelection, ActorSystem}
import akka.http.javadsl.server.PathMatcher1
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.{ExceptionHandler, PathMatcher, PathMatcher0, Route}
import akka.pattern.ask
import akka.util.Timeout
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import hydra.avro.resource.SchemaResource
import hydra.common.config.ConfigSupport
import hydra.common.config.ConfigSupport._
import hydra.core.akka.SchemaRegistryActor
import hydra.core.akka.SchemaRegistryActor._
import hydra.core.http.{CorsSupport, RouteSupport}
import hydra.core.marshallers.GenericServiceResponse
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.apache.avro.SchemaParseException
import spray.json.{JsArray, JsObject, JsValue, RootJsonFormat}
import hydra.core.monitor.HydraMetrics.addPromHttpMetric
import hydra.kafka.consumer.KafkaConsumerProxy.{ListTopics, ListTopicsResponse}
import org.apache.kafka.common.PartitionInfo
import scalacache.cachingF
import scalacache._
import scalacache.guava.GuavaCache
import scalacache.modes.scalaFuture._
import akka.http.scaladsl.server.ExceptionHandler

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.immutable.Map
import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * A wrapper around Confluent's schema registry that facilitates schema registration and retrieval.
  *
  * Created by alexsilva on 2/13/16.
  */
class SchemasEndpoint(consumerProxy: ActorSelection)(implicit system: ActorSystem)
    extends RouteSupport
    with ConfigSupport
    with CorsSupport {

  private implicit val cache = GuavaCache[Map[String, Seq[PartitionInfo]]]


  implicit val endpointFormat: RootJsonFormat[SchemasEndpointResponse] = jsonFormat3(SchemasEndpointResponse.apply)
  implicit val v2EndpointFormat: RootJsonFormat[SchemasWithKeyEndpointResponse] = jsonFormat2(SchemasWithKeyEndpointResponse.apply)
  implicit val schemasWithTopicFormat: RootJsonFormat[SchemasWithTopicResponse] = jsonFormat2(SchemasWithTopicResponse.apply)
  implicit val batchSchemasFormat: RootJsonFormat[BatchSchemasResponse] = {
    val make: List[SchemasWithTopicResponse] => BatchSchemasResponse = BatchSchemasResponse.apply
    jsonFormat1(make)
  }
  implicit val timeout: Timeout = Timeout(3.seconds)

  private val schemaRegistryActor =
    system.actorOf(SchemaRegistryActor.props(applicationConfig))

  private val filterSystemTopics = (t: String) =>
    (t.startsWith("_") && showSystemTopics) || !t.startsWith("_")

  private val showSystemTopics = applicationConfig
    .getBooleanOpt("transports.kafka.show-system-topics")
    .getOrElse(false)

  override def route: Route = cors(settings) {
    handleExceptions(excptHandler) {
      extractExecutionContext { implicit ec =>
        pathPrefix("schemas") {
          get {
            pathEndOrSingleSlash {
              onSuccess(
                (schemaRegistryActor ? FetchSubjectsRequest)
                  .mapTo[FetchSubjectsResponse]
              ) { response =>
                addPromHttpMetric("", OK.toString, "/schemas")
                complete(OK, response.subjects)
              }
            } ~ path(Segment) { subject =>
              parameters('schema ?) { schemaOnly: Option[String] =>
                getSchema(includeKeySchema = false, subject, schemaOnly)
              }
            } ~ path(Segment / "versions") { subject =>
              onSuccess(
                (schemaRegistryActor ? FetchAllSchemaVersionsRequest(subject))
                  .mapTo[FetchAllSchemaVersionsResponse]
              ) { response =>
                addPromHttpMetric(subject, OK.toString, "/schemas/.../versions")
                complete(OK, response.versions.map(SchemasEndpointResponse(_)))
              }
            } ~ path(Segment / "versions" / IntNumber) { (subject, version) =>
              onSuccess(
                (schemaRegistryActor ? FetchSchemaVersionRequest(
                  subject,
                  version
                )).mapTo[FetchSchemaVersionResponse]
              ) { response =>
                addPromHttpMetric(subject, OK.toString, "/schemas/.../versions/" + version)
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
  }

  private val v2Route =
    pathPrefix("v2") {
      get {
        pathPrefix("schemas") {
          pathEndOrSingleSlash {
            extractExecutionContext { implicit ec =>
              onSuccess(topics) { topics =>
                getSchemas(topics.keys.toList)
              }
            }
          }
        } ~
        pathPrefix("schemas" / Segment) { subject =>
          pathEndOrSingleSlash {
            getSchema(includeKeySchema = true, subject, None)
          }
        }
      }
    }

  def getSchema(includeKeySchema: Boolean, subject: String, schemaOnly: Option[String]): Route = {
    onSuccess(
      (schemaRegistryActor ? FetchSchemaRequest(subject))
        .mapTo[FetchSchemaResponse]
    ) { response =>
      extractExecutionContext { implicit ec =>
        if (includeKeySchema) {
          addPromHttpMetric(subject, OK.toString, "/v2/schemas/")
          complete(OK, SchemasWithKeyEndpointResponse.apply(response))
        } else {
          val schemaResource = response.schemaResource
          addPromHttpMetric(subject, OK.toString, "/schema/")
          schemaOnly.map{_ =>
            complete(OK, schemaResource.schema.toString)}
            .getOrElse {
              complete(OK, SchemasEndpointResponse(schemaResource))
            }
        }
      }
    }
  }

  def getSchemas(subjects: List[String]): Route = {
    onSuccess(
      (schemaRegistryActor ? FetchSchemasRequest(subjects))
        .mapTo[FetchSchemasResponse]
    ) {
      response => {
        extractExecutionContext { implicit ec =>
          addPromHttpMetric("",OK.toString, "/schemas")
          complete(OK, BatchSchemasResponse.apply(response))
        }
      }
    }
  }

  private def registerNewSchema: Route = {
    entity(as[String]) { json =>
      extractExecutionContext { implicit ec =>
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
              addPromHttpMetric(registeredSchema.schemaResource.schema.getFullName, Created.toString, "/schemas")
              complete(Created, SchemasEndpointResponse(registeredSchema))
            }
          }
        }
      }
    }
  }

  private[http] val excptHandler: ExceptionHandler = ExceptionHandler {
    case e: RestClientException if e.getErrorCode == 40401 =>
      extractExecutionContext { implicit ec =>
        addPromHttpMetric("", NotFound.toString, "schemasEndpoint")
        complete(NotFound, GenericServiceResponse(404, e.getMessage))
      }

    case e: RestClientException =>
      val registryHttpStatus = e.getStatus
      val registryErrorCode = e.getErrorCode
      extractExecutionContext { implicit ec =>
        addPromHttpMetric("", registryHttpStatus.toString, "schemasEndpoint")
        complete(
          registryHttpStatus,
          GenericServiceResponse(
            registryErrorCode,
            s"Registry error: ${e.getMessage}"
          )
        )
      }

    case e: SchemaParseException =>
      extractExecutionContext { implicit ec =>
        addPromHttpMetric("", BadRequest.toString, "schemasEndpoint")
        complete(
          BadRequest,
          GenericServiceResponse(
            400,
            s"Unable to parse avro schema: ${e.getMessage}"
          )
        )
      }

    case e: Exception =>
      extractExecutionContext { implicit ec =>
        extractUri { uri =>
          log.warn(s"Request to $uri failed with exception: {}", e)
          addPromHttpMetric("", BadRequest.toString, "schemasEndpoint")
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

  private def topics(implicit ec: ExecutionContext): Future[Map[String, Seq[PartitionInfo]]] = {
    implicit val timeout = Timeout(5 seconds)
    cachingF("topics")(ttl = Some(1.minute)) {
      import akka.pattern.ask
      (consumerProxy ? ListTopics).mapTo[ListTopicsResponse].map { response =>
        response.topics.filter(t => filterSystemTopics(t._1)).map {
          case (k, v) => k -> v.toList
        }
      }
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

case class SchemasWithTopicResponse(topic: String, valueSchemaResponse: Option[SchemasEndpointResponse])

object SchemasWithTopicResponse {
  def apply(t: (String, Option[SchemaResource])): SchemasWithTopicResponse =
    new SchemasWithTopicResponse(t._1, t._2.map(SchemasEndpointResponse.apply))
}

case class BatchSchemasResponse(schemasResponse: List[SchemasWithTopicResponse])

object BatchSchemasResponse {
  def apply(f: FetchSchemasResponse) =
    new BatchSchemasResponse(f.valueSchemas.map(SchemasWithTopicResponse.apply))
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
