package hydra.ingest.bootstrap

import akka.actor.ActorSystem
import akka.http.scaladsl.server
import akka.http.scaladsl.server.RouteConcatenation
import hydra.common.config.ConfigSupport
import hydra.common.util.ActorUtils
import hydra.ingest.http.{
  IngestionEndpoint,
  IngestionWebSocketEndpoint,
  IngestorRegistryEndpoint,
  SchemasEndpoint
}
import hydra.kafka.consumer.KafkaConsumerProxy
import hydra.kafka.endpoints.{
  BootstrapEndpoint,
  TopicMetadataEndpoint,
  TopicsEndpoint
}

object RouteFactory extends RouteConcatenation with ConfigSupport {

  def getRoutes()(implicit system: ActorSystem): server.Route = {
    import configs.syntax._

    //TODO: remove this lookup
    val consumerPath = applicationConfig
      .get[String]("actors.kafka.consumer_proxy.path")
      .valueOrElse(
        s"/user/service/${ActorUtils.actorName(classOf[KafkaConsumerProxy])}"
      )

    val consumerProxy = system.actorSelection(consumerPath)

    new SchemasEndpoint().route ~
      new BootstrapEndpoint(system).route ~
      new TopicMetadataEndpoint(consumerProxy)(system.dispatcher).route ~
      new IngestorRegistryEndpoint().route ~
      new IngestionWebSocketEndpoint().route ~
      new IngestionEndpoint().route ~
      new TopicsEndpoint(consumerProxy)(system.dispatcher).route
  }
}
