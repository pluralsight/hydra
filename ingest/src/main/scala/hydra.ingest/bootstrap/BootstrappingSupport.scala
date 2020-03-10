package hydra.ingest.bootstrap

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.RouteDirectives
import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import com.typesafe.config.ConfigFactory
import hydra.avro.registry.SchemaRegistry
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.core.http.RouteSupport
import hydra.ingest.app.AppConfig
import hydra.kafka.endpoints.BootstrapEndpointV2
import hydra.kafka.programs.CreateTopicProgram
import hydra.kafka.util.KafkaClient
import hydra.kafka.util.KafkaUtils.TopicDetails
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import retry.{RetryPolicies, RetryPolicy}

import scala.concurrent.ExecutionContext

class BootstrapEndpoints(
    implicit val system: ActorSystem,
    implicit val ec: ExecutionContext
) extends RouteSupport {

  private implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  private implicit val contextShift: ContextShift[IO] =
    IO.contextShift(ExecutionContext.global)
  private implicit val concurrent: ConcurrentEffect[IO] = IO.ioConcurrentEffect

  private implicit val logger: Logger[IO] = Slf4jLogger.getLogger

  private val config = AppConfig.appConfig.load[IO].unsafeRunSync()

  private val schemaRegistryUrl =
    config.createTopicConfig.schemaRegistryConfig.fullUrl

  private val bootstrapServers =
    config.createTopicConfig.bootstrapServers

  private val schemaRegistry =
    SchemaRegistry.live[IO](schemaRegistryUrl, 100).unsafeRunSync()

  private val ingestorSelection =
    system.actorSelection(
      path = ConfigFactory.load().getString("hydra.kafka-ingestor-path")
    )

  private val kafkaClient =
    KafkaClient.live[IO](bootstrapServers, ingestorSelection).unsafeRunSync()

  private val isBootstrapV2Enabled =
    config.v2MetadataTopicConfig.createV2TopicsEnabled

  private val v2MetadataTopicName =
    config.v2MetadataTopicConfig.topicName

  private val topicDetails =
    TopicDetails(
      config.createTopicConfig.defaultNumPartions,
      config.createTopicConfig.defaultReplicationFactor
    )

  private val bootstrapV2Endpoint = {
    if (isBootstrapV2Enabled) {
      val retryPolicy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      new BootstrapEndpointV2(
        new CreateTopicProgram[IO](
          schemaRegistry,
          kafkaClient,
          retryPolicy,
          v2MetadataTopicName
        ),
        topicDetails
      ).route
    } else {
      RouteDirectives.reject
    }
  }

  override def route: Route = bootstrapV2Endpoint
}