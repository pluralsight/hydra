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
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import cats.syntax.all._
import fs2.kafka.{Header, Headers}
import hydra.common.config.ConfigSupport._
import hydra.common.util.Futurable
import hydra.core.http.RouteSupport
import hydra.core.ingest.RequestParams.HYDRA_KAFKA_TOPIC_PARAM
import hydra.core.ingest.{CorrelationIdBuilder, HydraRequest, IngestionReport, RequestParams}
import hydra.core.marshallers.GenericError
import hydra.core.monitor.HydraMetrics.addPromHttpMetric
import hydra.core.protocol._
import hydra.ingest.services.IngestionFlow.{AvroConversionAugmentedException, MissingTopicNameException, SchemaNotFoundAugmentedException}
import hydra.ingest.services.IngestionFlowV2.V2IngestRequest
import hydra.ingest.services.{IngestionFlow, IngestionFlowV2}
import hydra.kafka.algebras.KafkaClientAlgebra.PublishError
import hydra.kafka.model.TopicMetadataV2Request.Subject

import scala.collection.immutable.Map
import scala.util.{Failure, Success, Try}

class IngestionEndpoint[F[_]: Futurable](
                                          ingestionFlow: IngestionFlow[F],
                                          ingestionV2Flow: IngestionFlowV2[F]
                                        )(implicit system: ActorSystem) extends RouteSupport with HydraIngestJsonSupport {

  import hydra.ingest.bootstrap.RequestFactories._

  override val route: Route =
    handleExceptions(exceptionHandler("UnknownTopic")) {
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

  private def getV2ReponseCode(e: Throwable): (StatusCode, Option[String]) = e match {
    case PublishError.Timeout => (StatusCodes.RequestTimeout, None)
    case e: PublishError.RecordTooLarge => (StatusCodes.PayloadTooLarge, e.getMessage.some)
    case r: IngestionFlowV2.AvroConversionAugmentedException => (StatusCodes.BadRequest, r.message.some)
    case r: IngestionFlowV2.SchemaNotFoundAugmentedException => (StatusCodes.BadRequest, Try(r.schemaNotFoundException.getMessage).toOption)
    case e => (StatusCodes.InternalServerError, Try(e.getMessage).toOption)
  }

  private val correlationIdHeader = "ps-correlation-id"

  private def publishRequestV2(topic: String): Route =
    handleExceptions(exceptionHandler(topic)) {
      extractExecutionContext { implicit ec =>
        optionalHeaderValueByName(correlationIdHeader) { cIdOpt =>
          entity(as[V2IngestRequest]) { reqMaybeHeader =>
            val req = cIdOpt match {
              case Some(id) => reqMaybeHeader.copy(headers = Some(Headers.fromSeq(List(Header.apply(correlationIdHeader, id)))))
              case _ => reqMaybeHeader
            }
            Subject.createValidated(topic) match {
              case Some(t) =>
                onComplete(Futurable[F].unsafeToFuture(ingestionV2Flow.ingest(req, t))) {
                  case Success(resp) =>
                    addPromHttpMetric(topic, StatusCodes.OK.toString, "/v2/topics/.../records")
                    complete(resp)
                  case Failure(e) =>
                    val status = getV2ReponseCode(e)
                    addPromHttpMetric(topic, status._1.toString, "/v2/topics/.../records")
                    complete(status)
                }
              case None =>
                addPromHttpMetric(topic, StatusCodes.BadRequest.toString, "/v2/topics/.../records")
                complete(StatusCodes.BadRequest, Subject.invalidFormat)
            }
          }
        }
      }
    }

  private def publishFlow(hydraRequest: HydraRequest,topic: String): Route = {
    extractExecutionContext { implicit ec =>
      onComplete(Futurable[F].unsafeToFuture(ingestionFlow.ingest(hydraRequest))) {
        case Success(_) =>
          addPromHttpMetric(topic, StatusCodes.OK.toString(), "/ingest")
          complete(IngestionReport(hydraRequest.correlationId, Map("kafka_ingestor" -> IngestorCompleted), StatusCodes.OK.intValue))
        case Failure(PublishError.Timeout) =>
          val errorMsg =
            s"${hydraRequest.correlationId}: Ack:${hydraRequest.ackStrategy}; Validation: ${hydraRequest.validationStrategy};" +
              s" Metadata:${hydraRequest.metadata}; Payload: ${hydraRequest.payload} Ingestors: Alt-Ingest-Flow"
          log.error(s"Ingestion timed out for request $errorMsg")
          addPromHttpMetric(topic, StatusCodes.RequestTimeout.toString,"/ingest")
          val responseCode = StatusCodes.RequestTimeout
          complete(responseCode, IngestionReport(hydraRequest.correlationId, Map("kafka_ingestor" -> IngestorTimeout), responseCode.intValue))
        case Failure(e@PublishError.RecordTooLarge(actual, limit)) =>
          val errorMsg =
            s"${hydraRequest.correlationId}: Ack:${hydraRequest.ackStrategy}; Validation: ${hydraRequest.validationStrategy};" +
              s" Metadata:${hydraRequest.metadata}; Ingestors: Alt-Ingest-Flow"
          log.error(s"Record too large. Found $actual bytes when limit is $limit bytes $errorMsg")
          val responseCode = StatusCodes.PayloadTooLarge
          addPromHttpMetric(topic, responseCode.toString,"/ingest")
          complete(responseCode, IngestionReport(hydraRequest.correlationId, Map("kafka_ingestor" -> IngestorError(e)), responseCode.intValue))
        case Failure(_: MissingTopicNameException) =>
          // Yeah, a 404 is a bad idea, but that is what the old v1 flow does so we are keeping it the same
          addPromHttpMetric(topic, StatusCodes.NotFound.toString,"/ingest")
          val responseCode = StatusCodes.NotFound
          complete(responseCode, IngestionReport(hydraRequest.correlationId, Map(), responseCode.intValue))
        case Failure(r: AvroConversionAugmentedException) =>
          addPromHttpMetric(topic, StatusCodes.BadRequest.toString,"/ingest")
          complete(StatusCodes.BadRequest, IngestionReport(hydraRequest.correlationId, Map("kafka_ingestor" -> InvalidRequest(r)), StatusCodes.BadRequest.intValue))
        case Failure(e: SchemaNotFoundAugmentedException) =>
          addPromHttpMetric(topic, StatusCodes.BadRequest.toString,"/ingest")
          complete(StatusCodes.BadRequest, IngestionReport(hydraRequest.correlationId, Map("kafka_ingestor" -> InvalidRequest(e)), StatusCodes.BadRequest.intValue))
        case Failure(other) =>
          val responseCode = StatusCodes.ServiceUnavailable
          val errorMsg =
            s"Exception: $other; ${hydraRequest.correlationId}: Ack:${hydraRequest.ackStrategy}; Validation: ${hydraRequest.validationStrategy};" +
              s" Metadata:${hydraRequest.metadata}; Payload: ${hydraRequest.payload} Ingestors: Alt-Ingest-Flow"
          log.error(s"Ingestion failed for request $errorMsg")
          addPromHttpMetric(topic, StatusCodes.ServiceUnavailable.toString,"/ingest")
          complete(responseCode, IngestionReport(hydraRequest.correlationId, Map("kafka_ingestor" -> IngestorError(other)), responseCode.intValue))
      }
    }
  }

  private def publishRequest: Route = parameter("correlationId" ?) { cIdOpt =>
    extractRequest { req =>
        onSuccess(createRequest[HttpRequest](cIdOpt.getOrElse(cId), req)) { hydraRequest =>
          val topic = hydraRequest.metadataValue(HYDRA_KAFKA_TOPIC_PARAM).getOrElse("UnknownTopic")
          handleExceptions(exceptionHandler(topic)) {
            publishFlow(hydraRequest,topic)
          }
      }
    }
  }

  private def exceptionHandler(topic: String) = ExceptionHandler {
      case e: IllegalArgumentException =>
        extractExecutionContext { implicit ec =>
          if (applicationConfig
            .getBooleanOpt("hydra.ingest.shouldLog400s")
            .getOrElse(false)) {
            log.error("Ingestion 400 ERROR: " + e.getMessage)
          }
          addPromHttpMetric(topic, StatusCodes.BadRequest.toString,"ingestionEndpoint")
          complete(400, GenericError(400, e.getMessage))
        }

      case e =>
        extractExecutionContext { implicit ec =>
          addPromHttpMetric(topic, StatusCodes.InternalServerError.toString,"ingestionEndpoint")
          complete(500, GenericError(500, e.getMessage))
        }
  }
}
