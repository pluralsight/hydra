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

import akka.actor._
import akka.http.scaladsl.model.{HttpRequest, StatusCode, StatusCodes}
import akka.http.scaladsl.server.{ExceptionHandler, Rejection, Route}
import hydra.common.config.ConfigSupport._
import hydra.common.util.Futurable
import hydra.core.http.RouteSupport
import hydra.core.ingest.RequestParams.HYDRA_KAFKA_TOPIC_PARAM
import hydra.core.ingest.{CorrelationIdBuilder, HydraRequest, IngestionReport, RequestParams}
import hydra.core.marshallers.GenericError
import hydra.core.monitor.HydraMetrics
import hydra.core.protocol._
import hydra.ingest.bootstrap.HydraIngestorRegistryClient
import hydra.ingest.services.IngestionFlow.{AvroConversionAugmentedException, MissingTopicNameException, SchemaNotFoundAugmentedException}
import hydra.ingest.services.IngestionFlowV2.V2IngestRequest
import hydra.ingest.services.{IngestionFlow, IngestionFlowV2, IngestionHandlerGateway}
import hydra.kafka.algebras.KafkaClientAlgebra.PublishError

import scala.collection.immutable.Map
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import cats.implicits._
import hydra.kafka.model.TopicMetadataV2Request.Subject

import scala.util.{Failure, Success, Try}


/**
  * Created by alexsilva on 12/22/15.
  */
class IngestionEndpoint[F[_]: Futurable](
                                          alternateIngestFlowEnabled: Boolean,
                                          ingestionFlow: IngestionFlow[F],
                                          ingestionV2Flow: IngestionFlowV2[F],
                                          useOldIngestIfUAContains: Set[String],
                                          requestHandlerPathName: Option[String] = None
                                        )(implicit system: ActorSystem) extends RouteSupport with HydraIngestJsonSupport {

  import hydra.ingest.bootstrap.RequestFactories._

  //for performance reasons, we give this endpoint its own instance of the gateway
  private val registryPath =
    HydraIngestorRegistryClient.registryPath(applicationConfig)

  private val requestHandler = system.actorOf(
    IngestionHandlerGateway.props(registryPath),
    requestHandlerPathName.getOrElse("ingestion_Http_handler_gateway")
  )

  private val ingestTimeout = applicationConfig
    .getDurationOpt("ingest.timeout")
    .getOrElse(500.millis)

  override val route: Route =
    handleExceptions(exceptionHandler) {
      pathPrefix("ingest") {
        pathEndOrSingleSlash {
          post {
            requestEntityPresent {
              publishRequest
            }
          } ~ deleteRequest
        }
      } ~
      pathPrefix("v2" / "topics" / Segment / "records") { topicName =>
        pathEndOrSingleSlash {
          post {
            publishRequestV2(topicName)
          }
        }
      }
    }

  private def deleteRequest = delete {
    headerValueByName(RequestParams.HYDRA_RECORD_KEY_PARAM)(_ => publishRequest)
  }

  private def cId = CorrelationIdBuilder.generate()

  private def useAlternateIngestFlow(request: HydraRequest): Boolean = {
    request.metadata.find(e => e._1.equalsIgnoreCase("User-Agent")) match {
      case Some(ua) if useOldIngestIfUAContains.exists(ua._2.startsWith) => false
      case _ => true
    }
  }

  private def addPromMetric(topic: String, responseCode: String)(implicit ec: ExecutionContext): Unit = {
    HydraMetrics.incrementGauge(
      lookupKey =
        "Ingest_topic_" + s"_${topic}",
      metricName = "ingest_topic_response",
      tags = Seq(
        "topic" -> topic,
        "responseCode" -> responseCode
      )
    )
  }

  private def getV2ReponseCode(e: Throwable): (StatusCode, Option[String]) = e match {
    case PublishError.Timeout => (StatusCodes.RequestTimeout, None)
    case r: IngestionFlowV2.AvroConversionAugmentedException => (StatusCodes.BadRequest, r.message.some)
    case r: IngestionFlowV2.SchemaNotFoundAugmentedException => (StatusCodes.BadRequest, Try(r.schemaNotFoundException.getMessage).toOption)
    case e => (StatusCodes.InternalServerError, Try(e.getMessage).toOption)
  }

  private def publishRequestV2(topic: String): Route =
    extractExecutionContext { implicit ec =>
      entity(as[V2IngestRequest]) { req =>
        Subject.createValidated(topic) match {
          case Some(t) =>
            onComplete(Futurable[F].unsafeToFuture(ingestionV2Flow.ingest(req, t))) {
              case Success(_) =>
                addPromMetric(topic, StatusCodes.OK.toString())
                complete(StatusCodes.OK)
              case Failure(e) =>
                val status = getV2ReponseCode(e)
                addPromMetric(topic, status.toString())
                complete(status)
            }
          case None => complete(StatusCodes.BadRequest, Subject.invalidFormat)
        }
      }
    }

  private def publishRequest: Route = parameter("correlationId" ?) { cIdOpt =>
    extractRequest { req =>
      extractExecutionContext { implicit executionContext =>
        onSuccess(createRequest[HttpRequest](cIdOpt.getOrElse(cId), req)) { hydraRequest =>
          val topic = hydraRequest.metadataValue(HYDRA_KAFKA_TOPIC_PARAM).getOrElse("UnknownTopic")
          if (alternateIngestFlowEnabled && useAlternateIngestFlow(hydraRequest)) {
            onComplete(Futurable[F].unsafeToFuture(ingestionFlow.ingest(hydraRequest))) {
              case Success(_) =>
                addPromMetric(topic, StatusCodes.OK.toString())
                complete(IngestionReport(hydraRequest.correlationId, Map("kafka_ingestor" -> IngestorCompleted), StatusCodes.OK.intValue))
              case Failure(PublishError.Timeout) =>
                val errorMsg =
                  s"${hydraRequest.correlationId}: Ack:${hydraRequest.ackStrategy}; Validation: ${hydraRequest.validationStrategy};" +
                    s" Metadata:${hydraRequest.metadata}; Payload: ${hydraRequest.payload} Ingestors: Alt-Ingest-Flow"
                log.error(s"Ingestion timed out for request $errorMsg")
                addPromMetric(topic, StatusCodes.RequestTimeout.toString())
                val responseCode = StatusCodes.RequestTimeout
                complete(responseCode, IngestionReport(hydraRequest.correlationId, Map("kafka_ingestor" -> IngestorJoined), responseCode.intValue))
              case Failure(_: MissingTopicNameException) =>
                // Yeah, a 404 is a bad idea, but that is what the old v1 flow does so we are keeping it the same
                addPromMetric(topic, StatusCodes.NotFound.toString())
                val responseCode = StatusCodes.NotFound
                complete(responseCode, IngestionReport(hydraRequest.correlationId, Map(), responseCode.intValue))
              case Failure(r: AvroConversionAugmentedException) =>
                addPromMetric(topic, StatusCodes.BadRequest.toString())
                complete(StatusCodes.BadRequest, IngestionReport(hydraRequest.correlationId, Map("kafka_ingestor" -> InvalidRequest(r)), StatusCodes.BadRequest.intValue))
              case Failure(e: SchemaNotFoundAugmentedException) =>
                addPromMetric(topic, StatusCodes.BadRequest.toString())
                complete(StatusCodes.BadRequest, IngestionReport(hydraRequest.correlationId, Map("kafka_ingestor" -> InvalidRequest(e)), StatusCodes.BadRequest.intValue))
              case Failure(other) =>
                val responseCode = StatusCodes.ServiceUnavailable
                val errorMsg =
                  s"Exception: $other; ${hydraRequest.correlationId}: Ack:${hydraRequest.ackStrategy}; Validation: ${hydraRequest.validationStrategy};" +
                    s" Metadata:${hydraRequest.metadata}; Payload: ${hydraRequest.payload} Ingestors: Alt-Ingest-Flow"
                log.error(s"Ingestion failed for request $errorMsg")
                addPromMetric(topic, StatusCodes.ServiceUnavailable.toString())
                complete(responseCode, IngestionReport(hydraRequest.correlationId, Map("kafka_ingestor" -> IngestorError(other)), responseCode.intValue))
            }
          } else {
            imperativelyComplete { ctx =>
              requestHandler ! InitiateHttpRequest(
                hydraRequest,
                ingestTimeout,
                ctx
              )
            }
          }
        }
      }
    }
  }

  private def exceptionHandler = ExceptionHandler {
    case e: IllegalArgumentException =>
      if (applicationConfig
            .getBooleanOpt("hydra.ingest.shouldLog400s")
            .getOrElse(false)) {
        log.error("Ingestion 400 ERROR: " + e.getMessage)
      }
      complete(400, GenericError(400, e.getMessage))
  }
}

case object DeleteDirectiveNotAllowedRejection extends Rejection
