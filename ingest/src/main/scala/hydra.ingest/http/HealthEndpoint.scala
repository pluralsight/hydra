package hydra.ingest.http

import java.time.Instant
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import hydra.common.util.Futurable
import hydra.core.http.RouteSupport
import hydra.ingest.bootstrap.BuildInfo
import spray.json.DefaultJsonProtocol
import hydra.core.monitor.HydraMetrics.addHttpMetric
import hydra.core.monitor.JVMStatsLogger._
import hydra.kafka.algebras.{ConsumerGroupsAlgebra, TagsAlgebra}

import scala.util.{Failure, Success}

class HealthEndpoint[F[_] : Futurable](cga: ConsumerGroupsAlgebra[F]) extends RouteSupport with DefaultJsonProtocol with SprayJsonSupport {

  private def addConsumerGroupStateToBuild(str: String, consumerGroup: String, state: Boolean) = {
    s"""{"BuildInfo": $str, "ConsumerGroupisActive": {"ConsumerGroupName": "$consumerGroup", "State": ${state}}}"""
  }

  override val route: Route = {
    extractMethod { method =>
      handleExceptions(exceptionHandler(Instant.now, method.value)) {
        path("health") {
          val startTime = Instant.now
          extractExecutionContext { implicit ec =>
            pathEndOrSingleSlash {
              get {
                addHttpMetric("", StatusCodes.OK, "/health", startTime, "GET")
                onComplete(
                  Futurable[F].unsafeToFuture(cga.consumerGroupIsActive(cga.getUniquePerNodeConsumerGroup))
                    .logJVMStats
                ) {
                  case Failure(exception) =>
                    addHttpMetric("", StatusCodes.InternalServerError, "/health", startTime, method.value, error = Some(exception.getMessage))
                    complete(StatusCodes.InternalServerError, BuildInfo.toJson + exception.getMessage)
                  case Success(value) =>
                    if (value._1) {
                      addHttpMetric("", StatusCodes.OK, "/health", startTime, method.value)
                      complete(StatusCodes.OK, addConsumerGroupStateToBuild(BuildInfo.toJson, value._2, value._1))
                    } else {
                      addHttpMetric("", StatusCodes.InternalServerError, "/health", startTime, method.value)
                      complete(StatusCodes.InternalServerError, addConsumerGroupStateToBuild(BuildInfo.toJson, value._2, value._1))
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
      extractExecutionContext { implicit ec =>
        addHttpMetric("", StatusCodes.InternalServerError, "/health", startTime, method, error = Some(e.getMessage))
        complete(500, e.getMessage)
      }
  }
}