package hydra.kafka.endpoints

import java.util.UUID
import akka.actor.{ActorRef, Props}
import com.typesafe.config.Config
import hydra.avro.registry.ConfluentSchemaRegistry
import hydra.common.config.KafkaConfigUtils
import hydra.common.config.KafkaConfigUtils.SchemaRegistrySecurityConfig
import hydra.core.akka.SchemaRegistryActor
import hydra.kafka.model.TopicMetadata
import hydra.kafka.services.{StreamsManagerActor, TopicBootstrapActor}
import hydra.kafka.util.KafkaUtils
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.joda.time.DateTime

trait BootstrapEndpointTestActors extends BootstrapEndpointActors {

  class StreamsActorTest(
      bootstrapKafkaConfig: Config,
      bootstrapServers: String,
      schemaRegistryClient: SchemaRegistryClient
  ) extends StreamsManagerActor(
        bootstrapKafkaConfig,
        KafkaConfigUtils.kafkaSecurityEmptyConfig,
        bootstrapServers,
        schemaRegistryClient
      ) {

    override val metadataMap: Map[String, TopicMetadata] =
      Map[String, TopicMetadata] {
        "exp.test-existing.v1.SubjectPreexisted" -> TopicMetadata(
          "exp.test-existing.v1.SubjectPreexisted",
          0,
          "",
          derived = false,
          None,
          "",
          "",
          None,
          None,
          UUID.randomUUID(),
          DateTime.now().minusSeconds(10),
          Some("notification.url")
        )
      }
  }

  object StreamsActorTest {

    def props(
        bootstrapKafkaConfig: Config,
        bootstrapServers: String,
        schemaRegistryClient: SchemaRegistryClient
    ) = {
      Props(
        new StreamsActorTest(
          bootstrapKafkaConfig,
          bootstrapServers,
          schemaRegistryClient
        )
      )
    }
  }

  override val bootstrapActor: ActorRef = system.actorOf(
    TopicBootstrapActor.props(
      schemaRegistryActor,
      kafkaIngestor,
      system.actorOf(StreamsActorTest.props(bootstrapKafkaConfig, KafkaUtils.BootstrapServers,
        ConfluentSchemaRegistry.forConfig(applicationConfig,  SchemaRegistrySecurityConfig(None, None)).registryClient)),
        KafkaConfigUtils.kafkaSecurityEmptyConfig,
        Some(bootstrapKafkaConfig)
    )
  )

}
