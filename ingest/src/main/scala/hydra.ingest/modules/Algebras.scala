package hydra.ingest.modules

import cats.effect.{Async, ConcurrentEffect, ContextShift, Timer}
import cats.syntax.all._
import hydra.avro.registry.SchemaRegistry
import hydra.ingest.app.AppConfig.AppConfig
import hydra.kafka.algebras.{ConsumerGroupsAlgebra, KafkaAdminAlgebra, KafkaClientAlgebra, MetadataAlgebra}
import io.chrisdavenport.log4cats.Logger

final class Algebras[F[_]] private (
    val schemaRegistry: SchemaRegistry[F],
    val kafkaAdmin: KafkaAdminAlgebra[F],
    val kafkaClient: KafkaClientAlgebra[F],
    val metadata: MetadataAlgebra[F],
    val consumerGroups: ConsumerGroupsAlgebra[F]
)

object Algebras {

  def make[F[_]: Async: ConcurrentEffect: ContextShift: Timer: Logger](config: AppConfig): F[Algebras[F]] =
    for {
      schemaRegistry <- SchemaRegistry.live[F](
        config.createTopicConfig.schemaRegistryConfig.fullUrl,
        config.createTopicConfig.schemaRegistryConfig.maxCacheSize
      )
      kafkaAdmin <- KafkaAdminAlgebra.live[F](config.createTopicConfig.bootstrapServers)
      kafkaClient <- KafkaClientAlgebra.live[F](config.createTopicConfig.bootstrapServers, schemaRegistry, config.ingestConfig.recordSizeLimitBytes)
      metadata <- MetadataAlgebra.make[F](config.v2MetadataTopicConfig.topicName.value,
        config.v2MetadataTopicConfig.consumerGroup, kafkaClient, schemaRegistry, config.v2MetadataTopicConfig.createOnStartup)
      consumerGroups <- ConsumerGroupsAlgebra.make[F](config.consumerGroupsAlgebraConfig.kafkaInternalConsumerGroupsTopic,
        config.dvsConsumersTopicConfig.topicName, config.consumerOffsetsOffsetsTopicConfig.topicName, config.createTopicConfig.bootstrapServers,
        config.v2MetadataTopicConfig.consumerGroup, config.consumerGroupsAlgebraConfig.commonConsumerGroup, kafkaClient, kafkaAdmin, schemaRegistry)
    } yield new Algebras[F](schemaRegistry, kafkaAdmin, kafkaClient, metadata, consumerGroups)
}
