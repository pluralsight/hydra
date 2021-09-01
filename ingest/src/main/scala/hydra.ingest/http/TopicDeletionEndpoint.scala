package hydra.ingest.http

import java.time.Instant
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.http.scaladsl.server.directives.Credentials
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import hydra.ingest.programs.{ActivelyPublishedToError, ConsumersStillExistError, DeleteTopicError, KafkaDeletionErrors, SchemaDeletionErrors, TopicDeletionProgram, TopicDoesNotExistError}
import hydra.common.util.Futurable
import hydra.core.http.RouteSupport
import spray.json._
import hydra.core.monitor.HydraMetrics.addHttpMetric
import hydra.ingest.programs.TopicDeletionProgram.SchemaDeleteTopicErrorList
import hydra.kafka.marshallers.ConsumerGroupMarshallers

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object TopicDeletionEndpoint extends
  DefaultJsonProtocol with
  SprayJsonSupport with ConsumerGroupMarshallers
{
  private implicit val endpointFormat = jsonFormat2(DeletionEndpointResponse.apply)
  final case class DeletionEndpointResponse(topicOrSubject: String, message: String)

  final case class DeletionRequest(topics: List[String], ignoreConsumerGroups: List[String])

  implicit object DeletionRequest extends RootJsonFormat[DeletionRequest] {
    override def read(json: JsValue): DeletionRequest = {
      json.asJsObject.getFields("topics", "ignoreConsumerGroups") match {
        case Seq(topics, ignoreConsumergroups) =>
          DeletionRequest(topics.convertTo[List[String]], ignoreConsumergroups.convertTo[List[String]])
        case Seq(topics) =>
          DeletionRequest(topics.convertTo[List[String]], List.empty)
        case _ =>
          spray.json.deserializationError("Must provide a List of topics to delete")
      }
    }

    override def write(obj: DeletionRequest): JsValue =
      Map("topics" -> obj.topics, "ignoreConsumerGroups" -> obj.ignoreConsumerGroups).toJson
  }

}

final class TopicDeletionEndpoint[F[_]: Futurable] (deletionProgram: TopicDeletionProgram[F], deletionPassword: String)
  extends RouteSupport with
    DefaultJsonProtocol with
    SprayJsonSupport {

  import TopicDeletionEndpoint._

  private def tupleErrorsToResponse(errorTuple: (List[SchemaDeletionErrors],
                                                 List[KafkaDeletionErrors],
                                                 List[ConsumersStillExistError],
                                                 List[ActivelyPublishedToError],
                                                 List[TopicDoesNotExistError])): List[DeletionEndpointResponse] = {
    schemaErrorsToResponse(errorTuple._1) ::: kafkaErrorsToResponse(errorTuple._2) ::: consumerErrorsToResponse(errorTuple._3) :::
        activelyPublishedToErrorsToResponse(errorTuple._4) ::: topicDoesNotExistErrorsToResponse(errorTuple._5)
  }

  private def schemaErrorsToResponse(schemaResults: List[SchemaDeletionErrors]): List[DeletionEndpointResponse] = {
    schemaResults.flatMap(_.schemaDeleteTopicErrorList.errors.map(e => DeletionEndpointResponse(e.getSubject, e.errorMessage)).toList)
  }

  private def kafkaErrorsToResponse(kafkaResults: List[KafkaDeletionErrors]): List[DeletionEndpointResponse] = {
    kafkaResults.flatMap(_.kafkaDeleteTopicErrorList.errors.map(e => DeletionEndpointResponse(e.topicName, e.errorMessage)).toList)
  }

  private def consumerErrorsToResponse(consumerError: List[ConsumersStillExistError]): List[DeletionEndpointResponse] = {
    consumerError.map(err => DeletionEndpointResponse(err.topic, s"The following consumers still exist for the topic: ${err.consumers.toJson.compactPrint}"))
  }

  private def activelyPublishedToErrorsToResponse(publishedToError: List[ActivelyPublishedToError]): List[DeletionEndpointResponse] = {
    publishedToError.map(err => DeletionEndpointResponse(err.topic,
      s"Cannot delete the requested topic because it has been published to within the last ${err.deleteWindow / 60000} minutes."))
  }

  private def topicDoesNotExistErrorsToResponse(topicDoesNotExistError: List[TopicDoesNotExistError]): List[DeletionEndpointResponse] = {
    topicDoesNotExistError.map(err => DeletionEndpointResponse(err.topic, "The requested topic does not exist."))
  }

  private def validResponse(topics: List[String], userDeleting: String, path: String, startTime: Instant)(implicit ec: ExecutionContext) = {
    topics.foreach { topicName =>
      addHttpMetric(topicName, StatusCodes.OK, path, startTime, "DELETE")
      log.info(s"User $userDeleting deleted topic: $topicName")
    }
    complete(StatusCodes.OK, topics)
  }

  private def returnResponse(topics: List[String], userDeleting: String, response: List[DeletionEndpointResponse],
                             path: String, startTime: Instant, responseCode: StatusCode)(implicit ec: ExecutionContext) = {
      topics.map(topicName => addHttpMetric(topicName, responseCode, path, startTime, "DELETE", error = Some(response.toString)))
      complete(responseCode, response)
  }

  private def invalidResponse(topics: List[String], userDeleting: String, e: NonEmptyList[DeleteTopicError],
                              path: String, startTime: Instant)(implicit ec: ExecutionContext) = {
    val allErrors = e.foldLeft((List.empty[SchemaDeletionErrors], List.empty[KafkaDeletionErrors], List.empty[ConsumersStillExistError],
      List.empty[ActivelyPublishedToError], List.empty[TopicDoesNotExistError])) { (agg, i) =>
      i match {
        case sde: SchemaDeletionErrors => (agg._1 :+ sde, agg._2, agg._3, agg._4, agg._5)
        case kde: KafkaDeletionErrors => (agg._1, agg._2 :+ kde, agg._3, agg._4, agg._5)
        case cde: ConsumersStillExistError => (agg._1, agg._2, agg._3 :+ cde, agg._4, agg._5)
        case apte: ActivelyPublishedToError => (agg._1, agg._2, agg._3, agg._4 :+ apte, agg._5)
        case tdne: TopicDoesNotExistError => (agg._1, agg._2, agg._3, agg._4, agg._5 :+ tdne)
      }
    }

    val responseCode: StatusCode = {
      if(topics.length != e.length || e.toList.exists(_.isInstanceOf[SchemaDeletionErrors])) StatusCodes.Accepted // we have something in topics that didn't throw an error, partial success
      else if (e.toList.exists(_.isInstanceOf[KafkaDeletionErrors])) StatusCodes.InternalServerError
      else if (e.toList.exists(_.isInstanceOf[TopicDoesNotExistError]) || e.toList.exists(_.isInstanceOf[ConsumersStillExistError]) ||
        e.toList.exists(_.isInstanceOf[ActivelyPublishedToError])) StatusCodes.BadRequest
      else StatusCodes.ImATeapot // Bill told me to do this
    }

    val response = tupleErrorsToResponse(allErrors)
    returnResponse(topics, userDeleting, response, path, startTime, responseCode)
  }

  private def deleteTopics(topics: List[String], ignoreConsumerGroups: List[String],
                           userDeleting: String, path: String, startTime: Instant)(implicit ec: ExecutionContext) = {
    onComplete(
      Futurable[F].unsafeToFuture(deletionProgram.deleteTopics(topics, ignoreConsumerGroups))
    ) {
      case Success(maybeSuccess) => {
        maybeSuccess match {
          case Validated.Valid(a) => {
            validResponse(topics, userDeleting, path, startTime)
          }
          case Validated.Invalid(e) => {
            invalidResponse(topics, userDeleting, e, path, startTime)
          }
        }
      }
      case Failure(e) => {
        topics.map(topicName => addHttpMetric(topicName, StatusCodes.InternalServerError, path, startTime,"DELETE", error = Option(e.getMessage)))
        complete(StatusCodes.InternalServerError, e.getMessage)
      }
    }
  }

  def myUserPassAuthenticator(credentials: Credentials): Option[String] =
    credentials match {
      case p@Credentials.Provided(id) if p.verify(deletionPassword) => Some(id)
      case _ => None
    }

  override val route: Route = {
    extractMethod { method =>
    handleExceptions(exceptionHandler(Instant.now, method.value)) {
      extractExecutionContext { implicit ec =>
        pathPrefix("v2" / "topics") {
          val startTime = Instant.now
          delete {
            authenticateBasic(realm = "", myUserPassAuthenticator) { userName =>
              pathPrefix("schemas" / Segment) { topic =>
                onComplete(
                  Futurable[F].unsafeToFuture(deletionProgram.deleteFromSchemaRegistry(List(topic)))
                ) {
                  case Success(maybeSuccess) => {
                    maybeSuccess match {
                      case Validated.Valid(a) => {
                        validResponse(List(topic), userName, "/v2/topics/schemas", startTime)
                      }
                      case Validated.Invalid(e) => {
                        val response = schemaErrorsToResponse(List(SchemaDeletionErrors(SchemaDeleteTopicErrorList(e))))
                        if (response.length >= 2) {
                          // With one topic coming in if we get anything >= to 2 there was at least a -key and a -value error
                          addHttpMetric(topic, StatusCodes.InternalServerError, "/v2/topics/schemas", startTime,"DELETE", error = Some(response.toString))
                          complete(StatusCodes.InternalServerError, response)
                        } else {
                          returnResponse(List(topic), userName, response, "/v2/topics/schemas", startTime, StatusCodes.Accepted)
                        }
                      }
                    }
                  }
                  case Failure(e) => {
                    addHttpMetric(topic, StatusCodes.InternalServerError, "/v2/topics/schemas", startTime,"DELETE", error = Some(e.getMessage))
                    complete(StatusCodes.InternalServerError, e.getMessage)
                  }
                }
              } ~
                pathPrefix(Segment) { topic =>
                  deleteTopics(List(topic), List.empty, userName, "/v2/topics", startTime)
                } ~
                pathEndOrSingleSlash {
                  entity(as[DeletionRequest]) { req =>
                    deleteTopics(req.topics, req.ignoreConsumerGroups, userName, "/v2/topics", startTime)
                  }
                }
            }
          }
        }
      }
    }
    }
  }

  private def exceptionHandler(startTime: Instant, method: String) = ExceptionHandler {
    case e =>
      extractExecutionContext{ implicit ec =>
        addHttpMetric("", StatusCodes.InternalServerError,"/v2/topics", startTime, method, error = Option(e.getMessage))
        complete(StatusCodes.InternalServerError, e.getMessage)
      }
  }
}
