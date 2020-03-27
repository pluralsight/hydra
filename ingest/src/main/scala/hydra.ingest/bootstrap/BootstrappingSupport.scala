package hydra.ingest.bootstrap

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.RouteDirectives
import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import com.typesafe.config.ConfigFactory
import fs2.kafka.{Deserializer, Serializer}
import fs2.kafka.vulcan.{AvroSettings, SchemaRegistryClientSettings, avroDeserializer, avroSerializer}
import hydra.avro.registry.SchemaRegistry
import hydra.core.http.RouteSupport
import hydra.ingest.app.AppConfig
import hydra.kafka.algebras.{KafkaAdminAlgebra, KafkaClientAlgebra}
import hydra.kafka.endpoints.BootstrapEndpointV2
import hydra.kafka.model.{TopicMetadataV2Key, TopicMetadataV2Value}
import hydra.kafka.programs.CreateTopicProgram
import hydra.kafka.util.KafkaUtils.TopicDetails
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import retry.{RetryPolicies, RetryPolicy}

import scala.concurrent.ExecutionContext

// $COVERAGE-OFF$
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

  private val kafkaAdmin =
    KafkaAdminAlgebra.live[IO](bootstrapServers).unsafeRunSync()

  private val kafkaClient =
    KafkaClientAlgebra.live[IO](bootstrapServers, schemaRegistry).unsafeRunSync()

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
          kafkaAdmin,
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

// $COVERAGE-ON