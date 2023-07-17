package hydra.kafka.endpoints

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.{ActorMaterializer, Materializer}
import hydra.avro.registry.ConfluentSchemaRegistry
import hydra.common.config.ConfigSupport
import hydra.common.config.KafkaConfigUtils.{KafkaClientSecurityConfig, SchemaRegistrySecurityConfig, kafkaSecurityEmptyConfig}
import hydra.core.akka.SchemaRegistryActor
import hydra.core.http.security.{AccessControlService, AwsSecurityService}
import hydra.kafka.services.{StreamsManagerActor, TopicBootstrapActor}
import hydra.kafka.util.KafkaUtils

import scala.concurrent.ExecutionContext

trait BootstrapEndpointActors[F[_]] extends ConfigSupport {

  implicit val system: ActorSystem
  implicit val streamsManagerActor: ActorRef

  private[kafka] val kafkaIngestor = system.actorSelection(path =
    applicationConfig.getString("kafka-ingestor-path")
  )

  private[kafka] val schemaRegistrySecurityConfig: SchemaRegistrySecurityConfig

  private[kafka] val schemaRegistryActor =
    system.actorOf(SchemaRegistryActor.props(applicationConfig, schemaRegistrySecurityConfig))

  private[kafka] val bootstrapKafkaConfig =
    applicationConfig.getConfig("bootstrap-config")

  private[kafka] val kafkaClientSecurityConfig: KafkaClientSecurityConfig = kafkaSecurityEmptyConfig

  private[kafka] val auth: AccessControlService[F]

  private[kafka] val awsSecurityService: AwsSecurityService[F]

  val bootstrapActor: ActorRef = system.actorOf(
    TopicBootstrapActor.props(
      schemaRegistryActor,
      kafkaIngestor,
      streamsManagerActor,
      kafkaClientSecurityConfig,
      Some(bootstrapKafkaConfig)
    )
  )

}
