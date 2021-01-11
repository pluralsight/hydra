package hydra.ingest.http

import java.time.Instant

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import hydra.core.http.RouteSupport
import hydra.ingest.bootstrap.BuildInfo
import spray.json.DefaultJsonProtocol
import hydra.core.monitor.HydraMetrics.addHttpMetric

object HealthEndpoint extends RouteSupport with DefaultJsonProtocol with SprayJsonSupport {
  override val route: Route = {
    handleExceptions(exceptionHandler(Instant.now)) {
      path("health") {
        val startTime = Instant.now
        extractExecutionContext { implicit ec =>
          pathEndOrSingleSlash {
            get {
              addHttpMetric("", StatusCodes.OK.toString, "/health", startTime, BuildInfo.toJson)
              complete(BuildInfo.toJson)
            }
          }
        }
      }
    }
  }

  private def exceptionHandler(startTime: Instant) = ExceptionHandler {
    case e =>
      extractExecutionContext { implicit ec =>
        addHttpMetric("", StatusCodes.InternalServerError.toString,"/health", startTime, e.getMessage, Some(e.getMessage))
        complete(500, e.getMessage)
      }
  }
}