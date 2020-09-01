package hydra.ingest.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import cats.data.Validated
import cats.implicits._
import hydra.common.util.Futurable
import hydra.core.http.RouteSupport
import spray.json.DefaultJsonProtocol
import hydra.core.monitor.HydraMetrics.addPromHttpMetric
import hydra.kafka.algebras.KafkaAdminAlgebra

import scala.util.{Failure, Success}

final class TopicDeletionEndpoint[F[_]: Futurable] (kafkaClient: KafkaAdminAlgebra[F]) extends RouteSupport with DefaultJsonProtocol with SprayJsonSupport {

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
                Futurable[F].unsafeToFuture(
                  kafkaClient.deleteTopics(maybeList)
                )
              ) {
                case Success(Right(_)) =>
                case Success(Left(deletionErrors)) =>
                case Failure(error) =>
              }

              // Confirm that topics are deleted?
              // If one or more failed continue and put in list for partial completion or failure
              // Call Schema registry and delete topic(s) that succeeded
              // return partial success if at least one topic succeeded
              // return failure if not topics succeeded
              // return success if all topics succeeded
//              maybeList.map(topic => addPromHttpMetric(topic, StatusCodes.OK.toString(), "/deleteTopics"))
//              complete(StatusCodes.OK)
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
