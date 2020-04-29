package hydra.ingest.modules

import cats.effect.{Async, ConcurrentEffect, ContextShift, Timer}
import cats.implicits._
import hydra.avro.registry.SchemaRegistry
import hydra.ingest.app.AppConfig.AppConfig
import hydra.kafka.algebras.{KafkaAdminAlgebra, KafkaClientAlgebra, MetadataAlgebra}

final class Algebras[F[_]] private (
    val schemaRegistry: SchemaRegistry[F],
    val kafkaAdmin: KafkaAdminAlgebra[F],
    val kafkaClient: KafkaClientAlgebra[F],
    val metadata: MetadataAlgebra[F]
)

object Algebras {

  def make[F[_]: Async: ConcurrentEffect: ContextShift: Timer](config: AppConfig): F[Algebras[F]] =
    for {
      schemaRegistry <- SchemaRegistry.live[F](
        config.createTopicConfig.schemaRegistryConfig.fullUrl,
        config.createTopicConfig.schemaRegistryConfig.maxCacheSize
      )
      kafkaAdmin <- KafkaAdminAlgebra.live[F](config.createTopicConfig.bootstrapServers)
      kafkaClient <- KafkaClientAlgebra.live[F](config.createTopicConfig.bootstrapServers, schemaRegistry)
      metadata <- MetadataAlgebra.make[F](config.v2MetadataTopicConfig.topicName.value,
        config.v2MetadataTopicConfig.consumerGroup, kafkaClient, config.v2MetadataTopicConfig.createOnStartup)
    } yield new Algebras[F](schemaRegistry, kafkaAdmin, kafkaClient, metadata)
}
