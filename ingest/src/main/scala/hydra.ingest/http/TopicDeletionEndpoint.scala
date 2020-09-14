package hydra.ingest.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import cats.data.Validated
import hydra.ingest.programs.{KafkaDeletionErrors, SchemaDeletionErrors, TopicDeletionProgram}
import hydra.common.util.Futurable
import hydra.core.http.RouteSupport
import spray.json.{DefaultJsonProtocol, JsArray, JsObject, JsValue, JsonFormat, RootJsonFormat}
import hydra.core.monitor.HydraMetrics.addPromHttpMetric

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

final class TopicDeletionEndpoint[F[_]: Futurable] (deletionProgram: TopicDeletionProgram[F])
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

  final case class DeletionRequest(valPayload: String)
  private final case class IntermediateDeletionRequest(topics: JsArray)
  private implicit val intermediateDeletionRequestFormat: JsonFormat[IntermediateDeletionRequest] =
    jsonFormat1(IntermediateDeletionRequest)

  implicit object DeletionRequestFormat extends RootJsonFormat[DeletionRequest] {
    override def read(json: JsValue): DeletionRequest = {
      val inter = intermediateDeletionRequestFormat.read(json)
      DeletionRequest(inter.topics.compactPrint)
    }

    //Not implemented on purpose. Never used
    override def write(obj: DeletionRequest): JsValue = ???
  }

  private def deleteTopics(topics: List[String])(implicit ec: ExecutionContext) = {
    onComplete(
      Futurable[F].unsafeToFuture(deletionProgram.deleteTopic(topics))
    ) {
      case Success(maybeSuccess) => {
        maybeSuccess match {
          case Validated.Valid(a) => {
            topics.map(topicName => addPromHttpMetric(topicName, StatusCodes.OK.toString(), "/deleteTopics/"))
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
              response.map(der => addPromHttpMetric(der.topicOrSubject, StatusCodes.Accepted.toString(), "/deleteTopics/"))
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

  override val route: Route =
    handleExceptions(exceptionHandler) {
      extractExecutionContext { implicit ec =>
        pathPrefix("v2" / "topics") {
          pathEndOrSingleSlash {
            delete {
              entity(as[DeletionRequest]) { req =>
                val maybeList = req.valPayload
                  .replace("[","")
                  .replace("\"","")
                  .replace(" ","")
                  .replace("]","").split(",").toList
                // check if consumers exist for this topic, if they do fail and return consumer groups
                // try deleting topic
                deleteTopics(maybeList)
              }
            }
          }
          pathPrefix(Segment) { topic =>
            pathEndOrSingleSlash {
              deleteTopics(List(topic))
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
