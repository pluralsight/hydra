package hydra.ingest.http

import java.time.Instant

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import hydra.core.http.RouteSupport
import hydra.core.monitor.HydraMetrics.addHttpMetric
import spray.json.DefaultJsonProtocol

object BadUri extends RouteSupport with DefaultJsonProtocol with SprayJsonSupport {
  override val route: Route = {
    extractUri { uri =>
      extractMethod { method =>
        handleExceptions(exceptionHandler(Instant.now, method.value, uri.toString)) {
            extractExecutionContext { implicit ec =>
              val startTime = Instant.now
              addHttpMetric("", StatusCodes.NotFound, s"$uri", startTime, method.value)
              complete(StatusCodes.NotFound, s"Unknown Path Provided: $uri")
            }
          }
      }
    }
  }

  private def exceptionHandler(startTime: Instant, method: String, path: String) = ExceptionHandler {
    case e =>
      extractExecutionContext { implicit ec =>
        addHttpMetric("", StatusCodes.InternalServerError, path, startTime, method, error = Some(e.getMessage))
        complete(500, e.getMessage)
      }
  }
}

