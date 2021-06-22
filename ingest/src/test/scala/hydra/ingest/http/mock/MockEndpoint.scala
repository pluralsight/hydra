package hydra.ingest.http.mock

import java.time.Instant

import akka.actor.{ActorSelection, ActorSystem}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import hydra.avro.registry.ConfluentSchemaRegistry
import hydra.common.config.ConfigSupport
import hydra.common.util.ActorUtils
import hydra.ingest.http.SchemasEndpoint
import hydra.kafka.consumer.KafkaConsumerProxy
import hydra.kafka.services.StreamsManagerActor
import hydra.kafka.util.KafkaUtils
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

  private val bootstrapKafkaConfig =
    applicationConfig.getConfig("bootstrap-config")

  private val streamsManagerProps = StreamsManagerActor.props(
    bootstrapKafkaConfig,
    KafkaUtils.BootstrapServers,
    ConfluentSchemaRegistry.forConfig(applicationConfig).registryClient
  )
  private val streamsManagerActor = system.actorOf(streamsManagerProps)

  def throwRestClientException(
      statusCode: Int,
      errorCode: Int,
      errorMessage: String
  ): Future[Any] = {
    throw new RestClientException(errorMessage, statusCode, errorCode)
  }

  val schemaRouteExceptionHandler: ExceptionHandler = {
    new SchemasEndpoint(consumerProxy, streamsManagerActor).excptHandler(Instant.now, "MockEndpoint")
  }

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
