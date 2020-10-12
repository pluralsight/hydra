package hydra.ingest.modules

import cats.effect._
import cats.syntax.all._
import hydra.ingest.app.AppConfig.AppConfig
import hydra.ingest.programs.TopicDeletionProgram
import hydra.ingest.services.{IngestionFlow, IngestionFlowV2}
import hydra.kafka.programs.CreateTopicProgram
import io.chrisdavenport.log4cats.Logger
import retry.RetryPolicies._
import retry.RetryPolicy
import scalacache.Mode

final class Programs[F[_]: Logger: Sync: Timer: Mode] private(
    cfg: AppConfig,
    algebras: Algebras[F]
) {

  val retryPolicy: RetryPolicy[F] =
    limitRetries[F](cfg.createTopicConfig.numRetries) |+| exponentialBackoff[F](
      cfg.createTopicConfig.baseBackoffDelay
    )

  val createTopic: CreateTopicProgram[F] = new CreateTopicProgram[F](
    algebras.schemaRegistry,
    algebras.kafkaAdmin,
    algebras.kafkaClient,
    retryPolicy,
    cfg.v2MetadataTopicConfig.topicName,
    algebras.metadata
  )

  val ingestionFlow: IngestionFlow[F] = new IngestionFlow[F](
    algebras.schemaRegistry,
    algebras.kafkaClient,
    cfg.createTopicConfig.schemaRegistryConfig.fullUrl
  )

  val ingestionFlowV2: IngestionFlowV2[F] = new IngestionFlowV2[F](
    algebras.schemaRegistry,
    algebras.kafkaClient,
    cfg.createTopicConfig.schemaRegistryConfig.fullUrl
  )

  val topicDeletion: TopicDeletionProgram[F] = new TopicDeletionProgram[F](
    algebras.kafkaAdmin,
    algebras.schemaRegistry
  )

}

object Programs {

  def make[F[_]: Logger: Sync: Timer: Mode](
      appConfig: AppConfig,
      algebras: Algebras[F]
  ): F[Programs[F]] = Sync[F].delay {
    new Programs[F](appConfig, algebras)
  }

}
