package hydra.ingest.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import hydra.core.http.RouteSupport
import hydra.ingest.bootstrap.BuildInfo
import spray.json.DefaultJsonProtocol
import hydra.core.monitor.HydraMetrics.addPromHttpMetric

object HealthEndpoint extends RouteSupport with DefaultJsonProtocol with SprayJsonSupport {
  override val route: Route =
    handleExceptions(exceptionHandler) {
      path("health") {
        extractExecutionContext { implicit ec =>
          pathEndOrSingleSlash {
            get {
              addPromHttpMetric("", StatusCodes.OK.toString, "/health")
              complete(BuildInfo.toJson)
            }
          }
        }
      }
    }

  private def exceptionHandler = ExceptionHandler {
    case e =>
      extractExecutionContext { implicit ec =>
        addPromHttpMetric("", StatusCodes.InternalServerError.toString,"/health")
        complete(500, e.getMessage)
      }
  }
}