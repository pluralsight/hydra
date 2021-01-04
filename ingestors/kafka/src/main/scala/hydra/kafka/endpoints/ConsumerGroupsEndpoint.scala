package hydra.kafka.endpoints

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.extractExecutionContext
import akka.http.scaladsl.server.Route
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import hydra.common.util.Futurable
import hydra.core.http.{CorsSupport, RouteSupport}
import hydra.core.monitor.HydraMetrics.addPromHttpMetric
import hydra.kafka.algebras.ConsumerGroupsAlgebra
import hydra.kafka.marshallers.ConsumerGroupMarshallers
import hydra.kafka.model.TopicMetadataV2Request.Subject

import scala.util.{Failure, Success}

class ConsumerGroupsEndpoint[F[_]: Futurable](consumerGroupsAlgebra: ConsumerGroupsAlgebra[F]) extends RouteSupport with CorsSupport with ConsumerGroupMarshallers {

  override def route: Route = cors(settings) {
    extractExecutionContext { implicit ec =>
      get {
        pathPrefix("v2" / "consumer-groups") {
          pathEndOrSingleSlash {
            onComplete(Futurable[F].unsafeToFuture(consumerGroupsAlgebra.getAllConsumers)) {
              case Success(consumers) =>
                addPromHttpMetric("", StatusCodes.OK.toString, "/v2/consumer-groups")
                complete(StatusCodes.OK, consumers)
              case Failure(exception) =>
                addPromHttpMetric("", StatusCodes.InternalServerError.toString, "/v2/consumer-groups")
                complete(StatusCodes.InternalServerError, exception.getMessage)
            }
          } ~ pathPrefix("getByTopic" / Segment) { topic =>
            pathEndOrSingleSlash {
              onComplete(
                Futurable[F].unsafeToFuture(consumerGroupsAlgebra.getConsumersForTopic(topic))
              ) {
                case Success(topicConsumers) =>
                  addPromHttpMetric(topic, StatusCodes.OK.toString, "/v2/consumer-groups/getByTopic")
                  complete(StatusCodes.OK, topicConsumers)
                case Failure(exception) =>
                  addPromHttpMetric(topic, StatusCodes.InternalServerError.toString, "/v2/consumer-groups/getByTopic")
                  complete(StatusCodes.InternalServerError, exception.getMessage)
              }
            }
          }
        } ~ pathPrefix("v2" / "topics" / "getByConsumerGroupName" / Segment) { consumerGroupName =>
          pathEndOrSingleSlash {
            onComplete(Futurable[F].unsafeToFuture(consumerGroupsAlgebra.getTopicsForConsumer(consumerGroupName))) {
              case Success(topics) =>
                addPromHttpMetric(consumerGroupName, StatusCodes.OK.toString, "/v2/topics/getByConsumerGroupName")
                complete(StatusCodes.OK, topics)
              case Failure(exception) =>
                addPromHttpMetric(consumerGroupName, StatusCodes.InternalServerError.toString, "/v2/topics/getByConsumerGroupName")
                complete(StatusCodes.InternalServerError, exception.getMessage)
            }
          }
        }
      }
    }
  }
}