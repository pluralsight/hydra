package hydra.kafka.endpoints

import java.time.Instant

import akka.http.scaladsl.marshallers.sprayjson._
import akka.http.scaladsl.model.StatusCodes
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.http.scaladsl.server.directives.Credentials
import hydra.common.util.Futurable
import hydra.core.http.RouteSupport
import hydra.core.monitor.HydraMetrics.addHttpMetric
import hydra.kafka.algebras.{HydraTag, TagsAlgebra}
import spray.json._
import hydra.core.http.CorsSupport

import scala.util.{Failure, Success}

final class SnowflakeEndpoint[F[_]: Futurable](snowflakeAlgebra: SnowflakeAlgebra[F])

  extends RouteSupport with DefaultJsonProtocol with SprayJsonSupport with CorsSupport {

  override val route: Route = cors(settings) {
    extractMethod { method =>
      handleExceptions(exceptionHandler(Instant.now, method.value)) {
        extractExecutionContext { implicit ec =>
          pathPrefix("v2" / "snowflakeTableNames") {
            get {
              val startTime = Instant.now
              onComplete(Futurable[F].unsafeToFuture(snowflakeAlgebra.snowflakeTableNames)) {
                case Failure(exception) =>
                  addHttpMetric("",StatusCodes.InternalServerError,"/v2/snowflakeTableNames", startTime, method.value, error = Some(exception.getMessage))
                  complete(StatusCodes.InternalServerError, exception.getMessage)
                case Success(value) =>
                  addHttpMetric("", StatusCodes.OK, "/v2/snowflakeTableNames", startTime, method.value)
                  complete(StatusCodes.OK, value.toJson)
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
        addHttpMetric("", StatusCodes.InternalServerError,"/v2/snowflakeTableNames", startTime, method, error = Some(e.getMessage))
        complete(StatusCodes.InternalServerError, e.getMessage)
      }
  }

}