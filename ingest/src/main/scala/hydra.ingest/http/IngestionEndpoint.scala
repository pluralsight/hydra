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
import hydra.core.http.security.AccessControlService
import hydra.core.ingest.RequestParams.HYDRA_KAFKA_TOPIC_PARAM
import hydra.core.ingest.{CorrelationIdBuilder, HydraRequest, IngestionReport, RequestParams}
import hydra.core.marshallers.GenericError
import hydra.core.monitor.HydraMetrics.addHttpMetric
import hydra.core.protocol._
import hydra.ingest.services.IngestionFlow.{AvroConversionAugmentedException, MissingTopicNameException, SchemaNotFoundAugmentedException}
import hydra.ingest.services.IngestionFlowV2.{KeyAndValueMismatchedValuesException, V2IngestRequest}
import hydra.ingest.services.{IngestionFlow, IngestionFlowV2}
import hydra.kafka.algebras.KafkaClientAlgebra.PublishError
import hydra.kafka.model.TopicMetadataV2Request.Subject
import org.apache.avro.AvroTypeException
import hydra.core.http.security.AwsIamPolicyAction.KafkaAction

import java.time.Instant
import scala.util.{Failure, Success, Try}

class IngestionEndpoint[F[_]: Futurable](
                                          ingestionFlow: IngestionFlow[F],
                                          ingestionV2Flow: IngestionFlowV2[F],
                                          auth: AccessControlService[F]
                                        )(implicit system: ActorSystem) extends RouteSupport with HydraIngestJsonSupport {

  import hydra.ingest.bootstrap.RequestFactories._

  override val route: Route = {
    extractMethod { method =>
      handleExceptions(exceptionHandler("UnknownTopic", Instant.now, method.value)) {
        pathPrefix("ingest") {
          val startTime = Instant.now
          pathEndOrSingleSlash {
            post {
              requestEntityPresent {
                publishRequest(startTime)
              }
            } ~ deleteRequest(startTime)
          }
        } ~
          pathPrefix("v2" / "topics" / Segment / "records") { topicName =>
            val startTime = Instant.now
            pathEndOrSingleSlash {
              post {
                auth.mskAuth(topicName, KafkaAction.WriteData) { _ =>
                  handleExceptions(exceptionHandler(topicName, Instant.now, method.value)) {
                    publishRequestV2(topicName, startTime)
                  }
                }
              }
            }
          }
      }
    }
  }

  private def deleteRequest(startTime: Instant) = delete {
    handleExceptions(exceptionHandler("UnknownTopic", Instant.now, "DELETE")) {
      headerValueByName(RequestParams.HYDRA_RECORD_KEY_PARAM)(_ => publishRequest(startTime))
    }
  }

  private def cId = CorrelationIdBuilder.generate()

  private def getV2ReponseCode(e: Throwable): (StatusCode, Option[String]) = e match {
    case PublishError.Timeout => (StatusCodes.RequestTimeout, None)
    case e: PublishError.RecordTooLarge => (StatusCodes.PayloadTooLarge, e.getMessage.some)
    case r: IngestionFlowV2.AvroConversionAugmentedException => (StatusCodes.BadRequest, r.message.some)
    case r: IngestionFlowV2.SchemaNotFoundAugmentedException => (StatusCodes.BadRequest, Try(r.schemaNotFoundException.getMessage).toOption)
    case t@(_: AvroTypeException | _: KeyAndValueMismatchedValuesException) => (StatusCodes.BadRequest, t.getMessage.some)
    case e => (StatusCodes.InternalServerError, Try(e.getMessage).toOption)
  }

  private val correlationIdHeader = "ps-correlation-id"

  private def publishRequestV2(topic: String, startTime: Instant): Route =
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
                  addHttpMetric(topic, StatusCodes.OK, "/v2/topics/.../records", startTime, "POST",
                    partitionOffset = Some(resp.partition, resp.offset.getOrElse(0)))
                  complete(resp)
                case Failure(e) =>
                  val status = getV2ReponseCode(e)
                  addHttpMetric(topic, status._1, "/v2/topics/.../records",
                    startTime, "POST", error = e.getMessage.some)
                  complete(status)
              }
            case None =>
              addHttpMetric(topic, StatusCodes.BadRequest,
                "/v2/topics/.../records", startTime, "POST", error = Subject.invalidFormat.some)
              complete(StatusCodes.BadRequest, Subject.invalidFormat)
          }
        }
      }
    }

  private def publishFlow(hydraRequest: HydraRequest,topic: String, startTime: Instant): Route = {
    extractExecutionContext { implicit ec =>
      onComplete(Futurable[F].unsafeToFuture(ingestionFlow.ingest(hydraRequest))) {
        case Success(_) =>
          addHttpMetric(topic, StatusCodes.OK, "/ingest", startTime, "POST")
          complete(IngestionReport(hydraRequest.correlationId, Map("kafka_ingestor" -> IngestorCompleted), StatusCodes.OK.intValue))
        case Failure(PublishError.Timeout) =>
          val responseCode = StatusCodes.RequestTimeout
          val errorMsg =
            s"${hydraRequest.correlationId}: Ack:${hydraRequest.ackStrategy}; Validation: ${hydraRequest.validationStrategy};" +
              s" Metadata:${hydraRequest.metadata}; Payload: ${hydraRequest.payload} Ingestors: Alt-Ingest-Flow"
          log.error(s"Ingestion timed out for request $errorMsg")
          addHttpMetric(topic, StatusCodes.RequestTimeout,"/ingest", startTime, "POST", error = s"Timeout - $errorMsg".some)
          complete(responseCode, IngestionReport(hydraRequest.correlationId, Map("kafka_ingestor" -> IngestorTimeout), responseCode.intValue))
        case Failure(e@PublishError.RecordTooLarge(actual, limit)) =>
          val responseCode = StatusCodes.PayloadTooLarge
          val errorMsg =
            s"${hydraRequest.correlationId}: Ack:${hydraRequest.ackStrategy}; Validation: ${hydraRequest.validationStrategy};" +
              s" Metadata:${hydraRequest.metadata}; Ingestors: Alt-Ingest-Flow"
          log.error(s"Record too large. Found $actual bytes when limit is $limit bytes $errorMsg")
          addHttpMetric(topic, responseCode,"/ingest", startTime, "POST", error = s"Record Too Large - $errorMsg".some)
          complete(responseCode, IngestionReport(hydraRequest.correlationId, Map("kafka_ingestor" -> IngestorError(e)), responseCode.intValue))
        case Failure(_: MissingTopicNameException) =>
          val responseCode = StatusCodes.NotFound
          // Yeah, a 404 is a bad idea, but that is what the old v1 flow does so we are keeping it the same
          addHttpMetric(topic, StatusCodes.NotFound,"/ingest", startTime, "POST", error = MissingTopicNameException.toString.some)
          complete(responseCode, IngestionReport(hydraRequest.correlationId, Map(), responseCode.intValue))
        case Failure(r: AvroConversionAugmentedException) =>
          addHttpMetric(topic, StatusCodes.BadRequest,"/ingest",startTime, "POST", error = AvroConversionAugmentedException.toString.some)
          complete(StatusCodes.BadRequest, IngestionReport(hydraRequest.correlationId,
            Map("kafka_ingestor" -> InvalidRequest(r)), StatusCodes.BadRequest.intValue))
        case Failure(e: SchemaNotFoundAugmentedException) =>
          addHttpMetric(topic, StatusCodes.BadRequest,"/ingest", startTime, "POST", error = s"Schema Not found ${e.getMessage}".some)
          complete(StatusCodes.BadRequest, IngestionReport(hydraRequest.correlationId,
            Map("kafka_ingestor" -> InvalidRequest(e)), StatusCodes.BadRequest.intValue))
        case Failure(other) =>
          val responseCode = StatusCodes.ServiceUnavailable
          val errorMsg =
            s"Exception: $other; ${hydraRequest.correlationId}: Ack:${hydraRequest.ackStrategy}; Validation: ${hydraRequest.validationStrategy};" +
              s" Metadata:${hydraRequest.metadata}; Payload: ${hydraRequest.payload} Ingestors: Alt-Ingest-Flow"
          log.error(s"Ingestion failed for request $errorMsg")
          addHttpMetric(topic, responseCode,"/ingest", startTime, "POST", error = s"Failure - $errorMsg".some)
          complete(responseCode, IngestionReport(hydraRequest.correlationId, Map("kafka_ingestor" -> IngestorError(other)), responseCode.intValue))
      }
    }
  }

  private def publishRequest(startTime: Instant): Route = parameter("correlationId" ?) { cIdOpt =>
    extractRequest { req =>
        onSuccess(createRequest[HttpRequest](cIdOpt.getOrElse(cId), req)) { hydraRequest =>
          val topic = hydraRequest.metadataValue(HYDRA_KAFKA_TOPIC_PARAM).getOrElse("UnknownTopic")
          handleExceptions(exceptionHandler(topic, startTime, "POST")) {
            publishFlow(hydraRequest,topic, startTime)
          }
      }
    }
  }

  private def exceptionHandler(topic: String, startTime: Instant, method: String) = ExceptionHandler {
      case e: IllegalArgumentException =>
        extractExecutionContext { implicit ec =>
          if (applicationConfig
            .getBooleanOpt("hydra.ingest.shouldLog400s")
            .getOrElse(false)) {
            log.error("Ingestion 400 ERROR: " + e.getMessage)
          }
          addHttpMetric(topic, StatusCodes.BadRequest,"ingestionEndpoint", startTime, method, error = e.getMessage.some)
          complete(400, GenericError(400, e.getMessage))
        }

      case e =>
        extractExecutionContext { implicit ec =>
          addHttpMetric(topic, StatusCodes.InternalServerError,"ingestionEndpoint", startTime, method, error = e.getMessage.some)
          complete(500, GenericError(500, e.getMessage))
        }
  }
}
