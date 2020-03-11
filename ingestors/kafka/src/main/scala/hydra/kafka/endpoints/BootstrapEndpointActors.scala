package hydra.kafka.endpoints

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.{ActorMaterializer, Materializer}
import hydra.avro.registry.ConfluentSchemaRegistry
import hydra.common.config.ConfigSupport
import hydra.core.akka.SchemaRegistryActor
import hydra.kafka.services.{StreamsManagerActor, TopicBootstrapActor}
import hydra.kafka.util.KafkaUtils

import scala.concurrent.ExecutionContext

trait BootstrapEndpointActors extends ConfigSupport {

  implicit val system: ActorSystem

  private[kafka] val kafkaIngestor = system.actorSelection(path =
    applicationConfig.getString("kafka-ingestor-path")
  )

  private[kafka] val schemaRegistryActor =
    system.actorOf(SchemaRegistryActor.props(applicationConfig))

  private[kafka] val bootstrapKafkaConfig =
    applicationConfig.getConfig("bootstrap-config")

  private[kafka] val streamsManagerProps = StreamsManagerActor.props(
    bootstrapKafkaConfig,
    KafkaUtils.BootstrapServers,
    ConfluentSchemaRegistry.forConfig(applicationConfig).registryClient
  )

  val bootstrapActor: ActorRef = system.actorOf(
    TopicBootstrapActor.props(
      schemaRegistryActor,
      kafkaIngestor,
      streamsManagerProps,
      Some(bootstrapKafkaConfig)
    )
  )

}
