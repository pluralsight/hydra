package hydra.kafka.endpoints

import java.time.Instant

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.extractExecutionContext
import akka.http.scaladsl.server.Route
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import hydra.common.util.Futurable
import hydra.core.http.{CorsSupport, RouteSupport}
import hydra.core.monitor.HydraMetrics.addHttpMetric
import hydra.kafka.algebras.ConsumerGroupsAlgebra
import hydra.kafka.marshallers.ConsumerGroupMarshallers
import hydra.kafka.model.TopicMetadataV2Request.Subject

import scala.util.{Failure, Success}

class ConsumerGroupsEndpoint[F[_]: Futurable](consumerGroupsAlgebra: ConsumerGroupsAlgebra[F]) extends RouteSupport with CorsSupport with ConsumerGroupMarshallers {

  override def route: Route = cors(settings) {
    extractExecutionContext { implicit ec =>
      get {
        pathPrefix("v2" / "consumer-groups") {
          val startTime = Instant.now
          pathEndOrSingleSlash {
            onComplete(Futurable[F].unsafeToFuture(consumerGroupsAlgebra.getAllConsumers)) {
              case Success(consumers) =>
                addHttpMetric("", StatusCodes.OK, "/v2/consumer-groups", startTime, "GET")
                complete(StatusCodes.OK, consumers)
              case Failure(exception) =>
                addHttpMetric("", StatusCodes.InternalServerError, "/v2/consumer-groups", startTime, "GET", error = Some(exception.getMessage))
                complete(StatusCodes.InternalServerError, exception.getMessage)
            }
          } ~ pathPrefix("v2" / "consumer-groups-by-topic") {
            val startTime = Instant.now
            pathEndOrSingleSlash {
              onComplete(Futurable[F].unsafeToFuture(consumerGroupsAlgebra.getAllConsumersByTopic)) {
                case Success(consumersByTopic) =>
                  addHttpMetric("", StatusCodes.OK, "/v2/consumer-groups-by-topic", startTime, "GET")
                  complete(StatusCodes.OK, consumersByTopic)
                case Failure(exception) =>
                  addHttpMetric("", StatusCodes.InternalServerError, "/v2/consumer-groups-by-topic", startTime, "GET", error = Some(exception.getMessage))
                  complete(StatusCodes.InternalServerError, exception.getMessage)
              }
            } ~ pathPrefix("getByTopic" / Segment) { topic =>
            pathEndOrSingleSlash {
              onComplete(
                Futurable[F].unsafeToFuture(consumerGroupsAlgebra.getConsumersForTopic(topic))
              ) {
                case Success(topicConsumers) =>
                  addHttpMetric(topic, StatusCodes.OK, "/v2/consumer-groups/getByTopic", startTime, "GET")
                  complete(StatusCodes.OK, topicConsumers)
                case Failure(exception) =>
                  addHttpMetric(topic, StatusCodes.InternalServerError, "/v2/consumer-groups/getByTopic", startTime, "GET", error = Some(exception.getMessage))
                  complete(StatusCodes.InternalServerError, exception.getMessage)
              }
            }
          }
        } ~ pathPrefix("v2" / "topics" / "getByConsumerGroupName" / Segment) { consumerGroupName =>
          val startTime = Instant.now
          pathEndOrSingleSlash {
            onComplete(Futurable[F].unsafeToFuture(consumerGroupsAlgebra.getTopicsForConsumer(consumerGroupName))) {
              case Success(topics) =>
                addHttpMetric(consumerGroupName, StatusCodes.OK, "/v2/topics/getByConsumerGroupName", startTime, "GET")
                complete(StatusCodes.OK, topics)
              case Failure(exception) =>
                addHttpMetric(consumerGroupName, StatusCodes.InternalServerError, "/v2/topics/getByConsumerGroupName", startTime, "GET", error = Some(exception.getMessage))
                complete(StatusCodes.InternalServerError, exception.getMessage)
            }
          }
        }
      }
    }
  }
}