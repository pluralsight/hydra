package hydra.ingest.http.mock

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import hydra.ingest.http.SchemasEndpoint
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException

import scala.concurrent.{ExecutionContext, Future}

class MockEndpoint(implicit system: ActorSystem, implicit val e: ExecutionContext)
  extends RoutedEndpoints {

  def throwRestClientException(statusCode: Int, errorCode: Int, errorMessage: String): Future[Any] = {
    throw new RestClientException(errorMessage, statusCode, errorCode)
  }

  val schemaRouteExceptionHandler: ExceptionHandler = new SchemasEndpoint().excptHandler

  override def route: Route = {
    pathPrefix("throwRestClientException") {
      handleExceptions(schemaRouteExceptionHandler) {
        get {
          parameters('statusCode, 'errorCode, 'errorMessage) { (statusCode, errorCode, errorMessage) =>
            pathEndOrSingleSlash {
              onSuccess(throwRestClientException(statusCode.toInt, errorCode.toInt, errorMessage)) { _ =>
                complete(OK)
              }
            }
          }
        }
      }
    }
  }
}

