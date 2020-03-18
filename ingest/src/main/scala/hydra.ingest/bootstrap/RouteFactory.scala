package hydra.ingest.bootstrap

import akka.actor.ActorSystem
import akka.http.scaladsl.server.{Route, RouteConcatenation}
import hydra.common.config.ConfigSupport
import hydra.common.util.ActorUtils
import hydra.ingest.http.{HealthEndpoint, IngestionEndpoint, IngestionWebSocketEndpoint, IngestorRegistryEndpoint, SchemasEndpoint}
import hydra.kafka.consumer.KafkaConsumerProxy
import hydra.kafka.endpoints.{BootstrapEndpoint, TopicMetadataEndpoint, TopicsEndpoint}

object RouteFactory extends RouteConcatenation with ConfigSupport {

  def getRoutes()(implicit system: ActorSystem): Route = {
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
      new BootstrapEndpoints()(system, system.dispatcher).route ~
      new TopicMetadataEndpoint(consumerProxy)(system.dispatcher).route ~
      new IngestorRegistryEndpoint().route ~
      new IngestionWebSocketEndpoint().route ~
      new IngestionEndpoint().route ~
      new TopicsEndpoint(consumerProxy)(system.dispatcher).route ~
      HealthEndpoint.route
  }
}
