package hydra.ingest.modules

import akka.actor.ActorSystem
import akka.http.scaladsl.server.directives.RouteDirectives
import akka.http.scaladsl.server.{Route, RouteConcatenation}
import cats.effect.Sync
import hydra.common.config.ConfigSupport
import hydra.common.util.{ActorUtils, Futurable}
import hydra.ingest.app.AppConfig.AppConfig
import hydra.ingest.http._
import hydra.kafka.consumer.KafkaConsumerProxy
import hydra.kafka.endpoints.{BootstrapEndpoint, BootstrapEndpointV2, TopicMetadataEndpoint, TopicsEndpoint}
import hydra.kafka.util.KafkaUtils.TopicDetails

final class Routes[F[_]: Sync: Futurable] private(programs: Programs[F], algebras: Algebras[F], cfg: AppConfig)
                                                 (implicit system: ActorSystem) extends RouteConcatenation with ConfigSupport {

  private val bootstrapEndpointV2 = if (cfg.v2MetadataTopicConfig.createV2TopicsEnabled) {
    val topicDetails =
      TopicDetails(
        cfg.createTopicConfig.defaultNumPartions,
        cfg.createTopicConfig.defaultReplicationFactor
      )
    new BootstrapEndpointV2(programs.createTopic, topicDetails, algebras.metadata).route
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

    new SchemasEndpoint().route ~
      new BootstrapEndpoint(system).route ~
      new TopicMetadataEndpoint(consumerProxy)(system.dispatcher).route ~
      new IngestorRegistryEndpoint().route ~
      new IngestionWebSocketEndpoint().route ~
      new IngestionEndpoint(cfg.ingestConfig.alternateIngestEnabled, programs.ingestionFlow, cfg.ingestConfig.useOldIngestIfUAContains).route ~
      new TopicsEndpoint(consumerProxy)(system.dispatcher).route ~
      HealthEndpoint.route ~
      bootstrapEndpointV2
  }
}

object Routes {
  def make[F[_]: Sync: Futurable](programs: Programs[F], algebras: Algebras[F], config: AppConfig)
                           (implicit system: ActorSystem): F[Routes[F]] = Sync[F].delay(new Routes[F](programs, algebras, config))
}
