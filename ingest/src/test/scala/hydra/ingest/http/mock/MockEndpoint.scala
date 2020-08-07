package hydra.ingest.http.mock

import akka.actor.{ActorSelection, ActorSystem}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import hydra.common.config.ConfigSupport
import hydra.common.util.ActorUtils
import hydra.ingest.http.SchemasEndpoint
import hydra.kafka.consumer.KafkaConsumerProxy
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException

import scala.concurrent.{ExecutionContext, Future}

class MockEndpoint(
    implicit system: ActorSystem,
    implicit val e: ExecutionContext
) extends ConfigSupport {

  import ConfigSupport._

  val consumerPath: String = applicationConfig
    .getStringOpt("actors.kafka.consumer_proxy.path")
    .getOrElse(
      s"/user/service/${ActorUtils.actorName(classOf[KafkaConsumerProxy])}"
    )

  val consumerProxy: ActorSelection = system.actorSelection(consumerPath)

  def throwRestClientException(
      statusCode: Int,
      errorCode: Int,
      errorMessage: String
  ): Future[Any] = {
    throw new RestClientException(errorMessage, statusCode, errorCode)
  }

  val schemaRouteExceptionHandler: ExceptionHandler =
    new SchemasEndpoint(consumerProxy).excptHandler

  def route: Route = {
    pathPrefix("throwRestClientException") {
      handleExceptions(schemaRouteExceptionHandler) {
        get {
          parameters('statusCode, 'errorCode, 'errorMessage) {
            (statusCode, errorCode, errorMessage) =>
              pathEndOrSingleSlash {
                onSuccess(
                  throwRestClientException(
                    statusCode.toInt,
                    errorCode.toInt,
                    errorMessage
                  )
                ) { _ => complete(OK) }
              }
          }
        }
      }
    }
  }
}
