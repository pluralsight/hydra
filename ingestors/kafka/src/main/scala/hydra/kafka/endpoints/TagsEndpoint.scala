package hydra.kafka.endpoints

import java.time.Instant

import akka.http.scaladsl.marshallers.sprayjson._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.http.scaladsl.server.directives.Credentials
import hydra.common.util.Futurable
import hydra.core.http.RouteSupport
import hydra.core.monitor.HydraMetrics.addHttpMetric
import hydra.kafka.algebras.{HydraTag, KafkaClientAlgebra, TagsAlgebra}
import spray.json._

import scala.util.{Failure, Success}

final class TagsEndpoint[F[_]: Futurable]( tagsAlgebra: TagsAlgebra[F],
                                           tagsPassword: String,
                                           tagsTopic: String,
                                           kafkaClientAlgebra: KafkaClientAlgebra[F])
  extends RouteSupport with DefaultJsonProtocol with SprayJsonSupport {

  def myUserPassAuthenticator(credentials: Credentials): Option[String] =
    credentials match {
      case p@Credentials.Provided(id) if p.verify(tagsPassword) => Some(id)
      case _ => None
    }

  override val route: Route = {
    extractMethod { method =>
      handleExceptions(exceptionHandler(Instant.now, method.value)) {
        extractExecutionContext { implicit ec =>
          pathPrefix("v2" / "tags") {
            get {
              onComplete(Futurable[F].unsafeToFuture(tagsAlgebra.getAllTags)) {
                case Failure(exception) => complete(StatusCodes.InternalServerError, exception)
                case Success(value) => complete(StatusCodes.OK, value.toString)
              }
            } ~ post {
              authenticateBasic(realm = "", myUserPassAuthenticator) { userName =>
                entity(as[HydraTag]) { tags =>
                  onComplete(Futurable[F].unsafeToFuture(tagsAlgebra.createOrUpdateTag(tagsTopic, tags, kafkaClientAlgebra))) {
                    case Failure(exception) => complete(StatusCodes.InternalServerError, exception.getMessage)
                    case Success(value) => complete(StatusCodes.OK, tags.toString)
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
        complete(500, e.getMessage)
      }
  }

}
