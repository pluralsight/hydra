package hydra.ingest.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import cats.data.Validated
import hydra.ingest.programs.{DeleteTopicError, SchemaDeletionErrors, TopicDeletionProgram}
import hydra.common.util.Futurable
import hydra.core.http.RouteSupport
import spray.json.DefaultJsonProtocol
import hydra.core.monitor.HydraMetrics.addPromHttpMetric
import hydra.kafka.algebras.KafkaAdminAlgebra

import scala.util.{Failure, Success}

final class TopicDeletionEndpoint[F[_]: Futurable] (deletionProgram: TopicDeletionProgram[F])
  extends RouteSupport with
    DefaultJsonProtocol with
    SprayJsonSupport {

  override val route: Route =
    handleExceptions(exceptionHandler) {
      path("deleteTopics" / Segment) { topic =>
        extractExecutionContext { implicit ec =>
          pathEndOrSingleSlash {
            delete {
              val maybeList = topic.split(",").toList
              // check if consumers exist for this topic, if they do fail and return consumer groups
              // try deleting topic

              onComplete(
                Futurable[F].unsafeToFuture(deletionProgram.deleteTopic(maybeList))
              ) {
                case Success(maybeSuccess) => {
                  maybeSuccess match {
                    case Validated.Valid(a) => {
                      maybeList.map(topicName => addPromHttpMetric(topicName, StatusCodes.OK.toString(), "/deleteTopics/"))
                      complete(StatusCodes.OK)
                    }
                    case Validated.Invalid(e) => {
                      e.asInstanceOf[DeleteTopicError] match {
                        case SchemaDeletionErrors(schemaDeleteTopicErrorList) => {
                          val badTopics = schemaDeleteTopicErrorList.errors.toList.map(error => error.subject.split("-").head)
                          val goodTopics = maybeList.toSet.diff(badTopics.toSet).toList
                          badTopics.map(topicName => addPromHttpMetric(topicName, StatusCodes.InternalServerError.toString(), "/deleteTopics/"))
                          goodTopics.map(topicName => addPromHttpMetric(topicName, StatusCodes.OK.toString(), "/deleteTopics/"))
                          complete(StatusCodes.Accepted, schemaDeleteTopicErrorList.errors.toList.map(error => error.errorMessage))
                        }
                      }
                    }
                  }
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
