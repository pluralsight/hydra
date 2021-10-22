package hydra.kafka.endpoints

import java.time.Instant
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.extractExecutionContext
import akka.http.scaladsl.server.Route
import cats.effect.IO
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import hydra.common.util.Futurable
import hydra.core.http.{CorsSupport, DefaultCorsSupport, RouteSupport}
import hydra.core.monitor.HydraMetrics.addHttpMetric
import hydra.kafka.algebras.KafkaAdminAlgebra.TopicAndPartition
import hydra.kafka.algebras.{ConsumerGroupsAlgebra, KafkaAdminAlgebra, KafkaClientAlgebra}
import hydra.kafka.marshallers.ConsumerGroupMarshallers
import hydra.kafka.model.TopicMetadataV2Request.Subject
import spray.json.JsString

import scala.util.{Failure, Success}

class ConsumerGroupsEndpoint[F[_]: Futurable](consumerGroupsAlgebra: ConsumerGroupsAlgebra[F])(implicit corsSupport: CorsSupport) extends RouteSupport with DefaultCorsSupport with ConsumerGroupMarshallers {

  override def route: Route = cors(corsSupport.settings) {
    extractExecutionContext { implicit ec =>
      extractMethod { method =>
        get {
          pathPrefix("v2" / "consumer-groups") {
            val startTime = Instant.now
            pathEndOrSingleSlash {
              onComplete(Futurable[F].unsafeToFuture(consumerGroupsAlgebra.getAllConsumers)) {
                case Success(consumers) =>
                  addHttpMetric("", StatusCodes.OK, "/v2/consumer-groups", startTime, method.value)
                  complete(StatusCodes.OK, consumers)
                case Failure(exception) =>
                  addHttpMetric("", StatusCodes.InternalServerError, "/v2/consumer-groups", startTime, method.value, error = Some(exception.getMessage))
                  complete(StatusCodes.InternalServerError, exception.getMessage)
              }
            } ~ pathPrefix(Segment) { consumerGroupName =>
              val startTime = Instant.now
              pathEndOrSingleSlash {
                onComplete(Futurable[F].unsafeToFuture(consumerGroupsAlgebra.getDetailedConsumerInfo(consumerGroupName))) {
                  case Success(detailedConsumer) =>
                    addHttpMetric(consumerGroupName, StatusCodes.OK, "/v2/consumer-groups/...", startTime, method.value)
                    complete(StatusCodes.OK, detailedConsumer)
                  case Failure(exception) =>
                    addHttpMetric(consumerGroupName, StatusCodes.InternalServerError, "/v2/consumer-groups/...", startTime, method.value, error = Some(exception.getMessage))
                    complete(StatusCodes.InternalServerError, exception.getMessage)
                }
              }
            }
          } ~ pathPrefix("v2" / "topic-consumer-groups") {
            val startTime = Instant.now
            pathEndOrSingleSlash {
              onComplete(Futurable[F].unsafeToFuture(consumerGroupsAlgebra.getAllConsumersByTopic)) {
                case Success(consumersByTopic) =>
                  addHttpMetric("", StatusCodes.OK, "/v2/topic-consumer-groups", startTime, method.value)
                  complete(StatusCodes.OK, consumersByTopic)
                case Failure(exception) =>
                  addHttpMetric("", StatusCodes.InternalServerError, "/v2/topic-consumer-groups", startTime, method.value, error = Some(exception.getMessage))
                  complete(StatusCodes.InternalServerError, exception.getMessage)
              }
            } ~ pathPrefix(Segment) { topic =>
              pathEndOrSingleSlash {
                onComplete(
                  Futurable[F].unsafeToFuture(consumerGroupsAlgebra.getConsumersForTopic(topic))
                ) {
                  case Success(topicConsumers) =>
                    addHttpMetric(topic, StatusCodes.OK, "/v2/consumer-groups/...", startTime, method.value)
                    complete(StatusCodes.OK, topicConsumers)
                  case Failure(exception) =>
                    addHttpMetric(topic, StatusCodes.InternalServerError, "/v2/consumer-groups/...", startTime, method.value, error = Some(exception.getMessage))
                    complete(StatusCodes.InternalServerError, exception.getMessage)
                }
              }
            }
          }
        }
      }
    }
  }
}