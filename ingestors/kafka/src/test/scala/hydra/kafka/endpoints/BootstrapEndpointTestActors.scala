package hydra.kafka.endpoints

import java.util.UUID

import akka.actor.{ActorRef, Props}
import com.typesafe.config.Config
import hydra.avro.registry.ConfluentSchemaRegistry
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
          DateTime.now().minusSeconds(10)
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

  private[kafka] val streamsManagerPropsTest = StreamsActorTest.props(
    bootstrapKafkaConfig,
    KafkaUtils.BootstrapServers,
    ConfluentSchemaRegistry.forConfig(applicationConfig).registryClient
  )

  override val bootstrapActor: ActorRef = system.actorOf(
    TopicBootstrapActor.props(
      schemaRegistryActor,
      kafkaIngestor,
      streamsManagerPropsTest,
      Some(bootstrapKafkaConfig)
    )
  )

}
