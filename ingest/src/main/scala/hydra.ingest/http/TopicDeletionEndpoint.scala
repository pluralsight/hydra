package hydra.ingest.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.directives.Credentials
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import cats.data.Validated
import hydra.ingest.programs.{KafkaDeletionErrors, SchemaDeletionErrors, TopicDeletionProgram}
import hydra.common.util.Futurable
import hydra.core.http.RouteSupport
import spray.json.{DefaultJsonProtocol, JsArray, JsObject, JsValue, JsonFormat, RootJsonFormat}
import hydra.core.monitor.HydraMetrics.addPromHttpMetric

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

final class TopicDeletionEndpoint[F[_]: Futurable] (deletionProgram: TopicDeletionProgram[F], deletionPassword: String)
  extends RouteSupport with
    DefaultJsonProtocol with
    SprayJsonSupport {

  implicit val endpointFormat = jsonFormat2(DeletionEndpointResponse.apply)
  case class DeletionEndpointResponse(topicOrSubject: String, message: String)

  private def tupleErrorsToResponse(errorTuple: (List[SchemaDeletionErrors], List[KafkaDeletionErrors])): List[DeletionEndpointResponse] = {
    schemaErrorsToResponse(errorTuple._1) ::: kafkaErrorsToResponse(errorTuple._2)
  }

  private def schemaErrorsToResponse(schemaResults: List[SchemaDeletionErrors]): List[DeletionEndpointResponse] = {
    schemaResults.flatMap(_.schemaDeleteTopicErrorList.errors.map(e => DeletionEndpointResponse(e.getSubject, e.errorMessage)).toList)
  }

  private def kafkaErrorsToResponse(kafkaResults: List[KafkaDeletionErrors]): List[DeletionEndpointResponse] = {
    kafkaResults.flatMap(_.kafkaDeleteTopicErrorList.errors.map(e => DeletionEndpointResponse(e.topicName, e.errorMessage)).toList)
  }

  final case class DeletionRequest(topics: List[String])
  implicit val deleteRequestFormat: RootJsonFormat[DeletionRequest] = jsonFormat1(DeletionRequest)

  private def deleteTopics(topics: List[String], userDeleting: String)(implicit ec: ExecutionContext) = {
    onComplete(
      Futurable[F].unsafeToFuture(deletionProgram.deleteTopic(topics))
    ) {
      case Success(maybeSuccess) => {
        maybeSuccess match {
          case Validated.Valid(a) => {
            topics.map(topicName => addPromHttpMetric(topicName, StatusCodes.OK.toString(), "/deleteTopics/"))
            topics.map(topicName => log.info(s"User $userDeleting deleted topic: $topicName"))
            complete(StatusCodes.OK, topics)
          }
          case Validated.Invalid(e) => {
            val allErrors = e.foldLeft((List.empty[SchemaDeletionErrors], List.empty[KafkaDeletionErrors])) { (agg, i) =>
              i match {
                case sde: SchemaDeletionErrors => (agg._1 :+ sde, agg._2)
                case kde: KafkaDeletionErrors => (agg._1, agg._2 :+ kde)
              }
            }
            val response = tupleErrorsToResponse(allErrors)
            if (response.length == topics.length) {
              topics.map(topicName => addPromHttpMetric(topicName, StatusCodes.InternalServerError.toString(), "/deleteTopics/"))
              complete(StatusCodes.InternalServerError, response)
            } else {
              val failedTopicNames = response.map(der => der.topicOrSubject)
              val successfulTopics = topics.toSet.diff(failedTopicNames.toSet).toList
              failedTopicNames.map(topicName => addPromHttpMetric(topicName, StatusCodes.Accepted.toString(), "/deleteTopics/"))
              successfulTopics.map(topicName => addPromHttpMetric(topicName, StatusCodes.OK.toString(), "/deleteTopics/"))
              successfulTopics.map(topicName => log.info(s"User $userDeleting deleted topic $topicName"))
              complete(StatusCodes.Accepted, response)
            }
          }
        }
      }
      case Failure(e) => {
        topics.map(topicName => addPromHttpMetric(topicName, StatusCodes.InternalServerError.toString(), "/deleteTopics/"))
        complete(StatusCodes.InternalServerError, e.getMessage)
      }
    }
  }

  def myUserPassAuthenticator(credentials: Credentials): Option[String] =
    credentials match {
      case p@Credentials.Provided(id) if p.verify(deletionPassword) => Some(id)
      case _ => None
    }

  override val route: Route =
    handleExceptions(exceptionHandler) {
      extractExecutionContext { implicit ec =>
        pathPrefix("v2" / "topics") {
          delete {
            authenticateBasic(realm = "secure site", myUserPassAuthenticator) { userName =>
              pathPrefix(Segment) { topic =>
                deleteTopics(List(topic), userName)
              } ~
              pathEndOrSingleSlash {
                entity(as[DeletionRequest]) { req =>
                  val maybeList = req.topics
                  // check if consumers exist for this topic, if they do fail and return consumer groups
                  // try deleting topic
                  deleteTopics(maybeList, userName)
                }
              }
            }
          }
        }
      }
    }

  private def exceptionHandler = ExceptionHandler {
    case e =>
      extractExecutionContext{ implicit ec =>
        addPromHttpMetric("", StatusCodes.InternalServerError.toString,"/deleteTopic")
        complete(500, e.getMessage)
      }
  }

}
