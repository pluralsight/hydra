package hydra.ingest.modules

import akka.actor.ActorSelection
import cats.effect.{Async, Concurrent, ContextShift}
import cats.implicits._
import hydra.avro.registry.SchemaRegistry
import hydra.ingest.app.AppConfig.{CreateTopicConfig, SchemaRegistryConfig}
import hydra.kafka.algebras.{KafkaAdminAlgebra, KafkaClientAlgebra}
import hydra.kafka.model.{TopicMetadataV2Key, TopicMetadataV2Value}

final class Algebras[F[_]] private (
    val schemaRegistry: SchemaRegistry[F],
    val kafkaAdmin: KafkaAdminAlgebra[F],
    val kafkaClient: KafkaClientAlgebra[F, TopicMetadataV2Key, TopicMetadataV2Value]
)

object Algebras {

  def make[F[_]: Async: Concurrent: ContextShift](createTopicConfig: CreateTopicConfig): F[Algebras[F]] =
    for {
      schemaRegistry <- SchemaRegistry.live[F](
        createTopicConfig.schemaRegistryConfig.fullUrl,
        createTopicConfig.schemaRegistryConfig.maxCacheSize
      )
      kafkaAdmin <- KafkaAdminAlgebra.live[F](createTopicConfig.bootstrapServers)
      kafkaClient <- KafkaClientAlgebra.live[F, TopicMetadataV2Key, TopicMetadataV2Value](createTopicConfig.bootstrapServers)
    } yield new Algebras[F](schemaRegistry, kafkaAdmin, kafkaClient)
}
