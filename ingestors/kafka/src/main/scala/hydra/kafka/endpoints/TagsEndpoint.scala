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

final class TagsEndpoint[F[_]: Futurable]( tagsAlgebra: TagsAlgebra[F],
                                           tagsPassword: String)
  extends RouteSupport with DefaultJsonProtocol with SprayJsonSupport with CorsSupport {

  def myUserPassAuthenticator(credentials: Credentials): Option[String] =
    credentials match {
      case p@Credentials.Provided(id) if p.verify(tagsPassword) => Some(id)
      case _ => None
    }

  override val route: Route = cors(settings) {
    extractMethod { method =>
      handleExceptions(exceptionHandler(Instant.now, method.value)) {
        extractExecutionContext { implicit ec =>
          pathPrefix("v2" / "tags") {
            get {
              val startTime = Instant.now
              onComplete(Futurable[F].unsafeToFuture(tagsAlgebra.getAllTags)) {
                case Failure(exception) =>
                  addHttpMetric("",StatusCodes.InternalServerError,"/v2/tags", startTime, method.value, error = Some(exception.getMessage))
                  complete(StatusCodes.InternalServerError, exception.getMessage)
                case Success(value) =>
                  addHttpMetric("", StatusCodes.OK, "/v2/tags", startTime, method.value)
                  complete(StatusCodes.OK, value.toJson)
              }
            } ~ post {
              val startTime = Instant.now
              authenticateBasic(realm = "", myUserPassAuthenticator) { userName =>
                entity(as[HydraTag]) { tags =>
                  onComplete(Futurable[F].unsafeToFuture(tagsAlgebra.createOrUpdateTag(tags))) {
                    case Failure(exception) =>
                      addHttpMetric("",StatusCodes.InternalServerError,"/v2/tags", startTime, method.value, error = Some(exception.getMessage))
                      complete(StatusCodes.InternalServerError, exception.getMessage)
                    case Success(value) =>
                      addHttpMetric("",StatusCodes.OK,"/v2/tags",startTime,method.value)
                      complete(StatusCodes.OK, tags.toString)
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
      extractExecutionContext{ implicit ec =>
        addHttpMetric("", StatusCodes.InternalServerError,"/v2/tags", startTime, method, error = Some(e.getMessage))
        complete(StatusCodes.InternalServerError, e.getMessage)
      }
  }

}
