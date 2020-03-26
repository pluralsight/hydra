package hydra.ingest.modules

import akka.actor.ActorSelection
import cats.effect.{Async, Concurrent, ConcurrentEffect, ContextShift, Timer}
import cats.implicits._
import hydra.avro.registry.SchemaRegistry
import hydra.ingest.app.AppConfig.{CreateTopicConfig, SchemaRegistryConfig}
import hydra.kafka.algebras.{KafkaAdminAlgebra, KafkaClientAlgebra}
import hydra.kafka.model.{TopicMetadataV2Key, TopicMetadataV2Value}

final class Algebras[F[_]] private (
    val schemaRegistry: SchemaRegistry[F],
    val kafkaAdmin: KafkaAdminAlgebra[F],
    val kafkaClient: KafkaClientAlgebra[F]
)

object Algebras {

  def make[F[_]: Async: ConcurrentEffect: ContextShift: Timer](createTopicConfig: CreateTopicConfig): F[Algebras[F]] =
    for {
      schemaRegistry <- SchemaRegistry.live[F](
        createTopicConfig.schemaRegistryConfig.fullUrl,
        createTopicConfig.schemaRegistryConfig.maxCacheSize
      )
      kafkaAdmin <- KafkaAdminAlgebra.live[F](createTopicConfig.bootstrapServers)
      kafkaClient <- KafkaClientAlgebra.live[F](createTopicConfig.bootstrapServers)
    } yield new Algebras[F](schemaRegistry, kafkaAdmin, kafkaClient)
}
