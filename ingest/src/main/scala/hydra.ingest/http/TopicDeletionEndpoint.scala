package hydra.ingest.http

import java.time.Instant

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.directives.Credentials
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import hydra.ingest.programs.{DeleteTopicError, KafkaDeletionErrors, SchemaDeletionErrors, TopicDeletionProgram}
import hydra.common.util.Futurable
import hydra.core.http.RouteSupport
import spray.json.{DefaultJsonProtocol, RootJsonFormat}
import hydra.core.monitor.HydraMetrics.addHttpMetric
import hydra.ingest.programs.TopicDeletionProgram.SchemaDeleteTopicErrorList

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object TopicDeletionEndpoint extends
  DefaultJsonProtocol with
  SprayJsonSupport
{
  private implicit val endpointFormat = jsonFormat2(DeletionEndpointResponse.apply)
  final case class DeletionEndpointResponse(topicOrSubject: String, message: String)

  final case class DeletionRequest(topics: List[String])
  implicit val deleteRequestFormat: RootJsonFormat[DeletionRequest] = jsonFormat1(DeletionRequest)
}

final class TopicDeletionEndpoint[F[_]: Futurable] (deletionProgram: TopicDeletionProgram[F], deletionPassword: String)
  extends RouteSupport with
    DefaultJsonProtocol with
    SprayJsonSupport {

  import TopicDeletionEndpoint._

  private def tupleErrorsToResponse(errorTuple: (List[SchemaDeletionErrors], List[KafkaDeletionErrors])): List[DeletionEndpointResponse] = {
    schemaErrorsToResponse(errorTuple._1) ::: kafkaErrorsToResponse(errorTuple._2)
  }

  private def schemaErrorsToResponse(schemaResults: List[SchemaDeletionErrors]): List[DeletionEndpointResponse] = {
    schemaResults.flatMap(_.schemaDeleteTopicErrorList.errors.map(e => DeletionEndpointResponse(e.getSubject, e.errorMessage)).toList)
  }

  private def kafkaErrorsToResponse(kafkaResults: List[KafkaDeletionErrors]): List[DeletionEndpointResponse] = {
    kafkaResults.flatMap(_.kafkaDeleteTopicErrorList.errors.map(e => DeletionEndpointResponse(e.topicName, e.errorMessage)).toList)
  }

  private def validResponse(topics: List[String], userDeleting: String, path: String, startTime: Instant)(implicit ec: ExecutionContext) = {
    topics.foreach { topicName =>
      addHttpMetric(topicName, StatusCodes.OK, path, startTime, "DELETE")
      log.info(s"User $userDeleting deleted topic: $topicName")
    }
    complete(StatusCodes.OK, topics)
  }

  private def returnResponse(topics: List[String], userDeleting: String, response: List[DeletionEndpointResponse],
                             path: String, startTime: Instant)(implicit ec: ExecutionContext) = {
    if(response.map(_.topicOrSubject.contains("-key")).contains(true) ||
      response.map(_.topicOrSubject.contains("-value")).contains(true)) {
      val failedTopicNames = response.map(der => der.topicOrSubject)
      val successfulTopics = topics.toSet.diff(failedTopicNames.toSet).toList
      failedTopicNames.map(topicName => addHttpMetric(topicName, StatusCodes.Accepted,
        path, startTime,"DELETE", error = Some(response.toString)))
      successfulTopics.foreach{ topicName =>
        addHttpMetric(topicName, StatusCodes.OK, path, startTime, "DELETE")
        log.info(s"User $userDeleting deleted topic $topicName")
      }
      complete(StatusCodes.Accepted, response)
    }
    else {
      topics.map(topicName => addHttpMetric(topicName, StatusCodes.InternalServerError, path, startTime, "DELETE"))
      complete(StatusCodes.InternalServerError, response)
    }
  }

  private def invalidResponse(topics: List[String], userDeleting: String, e: NonEmptyList[DeleteTopicError], path: String, startTime: Instant)(implicit ec: ExecutionContext) = {
    val allErrors = e.foldLeft((List.empty[SchemaDeletionErrors], List.empty[KafkaDeletionErrors])) { (agg, i) =>
      i match {
        case sde: SchemaDeletionErrors => (agg._1 :+ sde, agg._2)
        case kde: KafkaDeletionErrors => (agg._1, agg._2 :+ kde)
      }
    }
    val response = tupleErrorsToResponse(allErrors)
    returnResponse(topics, userDeleting, response, path, startTime)
  }

  private def deleteTopics(topics: List[String], userDeleting: String, path: String, startTime: Instant)(implicit ec: ExecutionContext) = {
    onComplete(
      Futurable[F].unsafeToFuture(deletionProgram.deleteTopic(topics))
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
        topics.map(topicName => addHttpMetric(topicName, StatusCodes.InternalServerError, path, startTime,"DELETE", error = Some(e.getMessage)))
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
    handleExceptions(exceptionHandler(Instant.now, extractMethod.toString)) {
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
                          returnResponse(List(topic), userName, response, "/v2/topics/schemas", startTime)
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
                  deleteTopics(List(topic), userName, "/v2/topics", startTime)
                } ~
                pathEndOrSingleSlash {
                  entity(as[DeletionRequest]) { req =>
                    deleteTopics(req.topics, userName, "/v2/topics", startTime)
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
        addHttpMetric("", StatusCodes.InternalServerError,"/v2/topics", startTime, method, error = Some(e.getMessage))
        complete(500, e.getMessage)
      }
  }

}
