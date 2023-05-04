package hydra.ingest.modules

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.server.directives.RouteDirectives
import akka.http.scaladsl.server.{Route, RouteConcatenation}
import cats.effect.Sync
import hydra.avro.registry.{ConfluentSchemaRegistry, RedisSchemaRegistryClient}
import hydra.common.config.ConfigSupport
import hydra.common.util.{ActorUtils, Futurable}
import hydra.core.http.CorsSupport
import hydra.ingest.app.AppConfig.AppConfig
import hydra.ingest.http._
import hydra.kafka.consumer.KafkaConsumerProxy
import hydra.kafka.endpoints._
import hydra.kafka.services.StreamsManagerActor
import hydra.kafka.util.KafkaUtils
import hydra.kafka.util.KafkaUtils.TopicDetails

import scala.concurrent.ExecutionContext

final class Routes[F[_]: Sync: Futurable] private(programs: Programs[F], algebras: Algebras[F], cfg: AppConfig)
                                                 (implicit system: ActorSystem, corsSupport: CorsSupport) extends RouteConcatenation with ConfigSupport {


  private implicit val ec: ExecutionContext = system.dispatcher
  private val bootstrapEndpointV2 = if (cfg.metadataTopicsConfig.createV2TopicsEnabled) {
    val topicDetails =
      TopicDetails(
        cfg.createTopicConfig.defaultNumPartions,
        cfg.createTopicConfig.defaultReplicationFactor,
        cfg.createTopicConfig.defaultMinInsyncReplicas
      )
    new BootstrapEndpointV2(programs.createTopic, topicDetails, algebras.tagsAlgebra).route
  } else {
    RouteDirectives.reject
  }

  lazy val routes: F[Route] = Sync[F].delay {
    import ConfigSupport._

    //TODO: remove this lookup
    val consumerPath = applicationConfig
      .getStringOpt("actors.kafka.consumer_proxy.path")
      .getOrElse(
        s"/user/service/${ActorUtils.actorName(classOf[KafkaConsumerProxy])}"
      )

    val consumerProxy = system.actorSelection(consumerPath)

     val bootstrapKafkaConfig =
    applicationConfig.getConfig("bootstrap-config")

    val registryClient =
      if (cfg.schemaRegistryRedisConfig.useRedisClient) {
        RedisSchemaRegistryClient.forConfig(applicationConfig, cfg.schemaRegistrySecurityConfig).registryClient
      } else {
        ConfluentSchemaRegistry.forConfig(applicationConfig, cfg.schemaRegistrySecurityConfig).registryClient
      }

      val streamsManagerProps = StreamsManagerActor.props(
      bootstrapKafkaConfig,
      cfg.kafkaClientSecurityConfig,
      KafkaUtils.BootstrapServers,
      registryClient
    )
     val streamsManagerActor: ActorRef = system.actorOf(streamsManagerProps, "streamsManagerActor")

    new SchemasEndpoint(consumerProxy, streamsManagerActor, cfg.schemaRegistrySecurityConfig).route ~
      new BootstrapEndpoint(system, streamsManagerActor, cfg.kafkaClientSecurityConfig, cfg.schemaRegistrySecurityConfig).route ~
      new TopicMetadataEndpoint(consumerProxy, algebras.metadata,
        algebras.schemaRegistry, programs.createTopic, cfg.createTopicConfig.defaultMinInsyncReplicas, algebras.tagsAlgebra).route ~
      new ConsumerGroupsEndpoint(algebras.consumerGroups).route ~
      new IngestorRegistryEndpoint().route ~
      new IngestionWebSocketEndpoint().route ~
      new IngestionEndpoint(programs.ingestionFlow, programs.ingestionFlowV2).route ~
      new TopicsEndpoint(consumerProxy, cfg.kafkaClientSecurityConfig)(system.dispatcher).route ~
      new TopicDeletionEndpoint(programs.topicDeletion,cfg.topicDeletionConfig.deleteTopicPassword).route ~
      new HealthEndpoint(algebras.consumerGroups).route ~
      new TagsEndpoint[F](algebras.tagsAlgebra, cfg.tagsConfig.tagsPassword).route ~
      bootstrapEndpointV2
  }
}

object Routes {
  def make[F[_]: Sync: Futurable](programs: Programs[F], algebras: Algebras[F], config: AppConfig)
                           (implicit system: ActorSystem, corsSupport: CorsSupport): F[Routes[F]] = Sync[F].delay(new Routes[F](programs, algebras, config))
}
