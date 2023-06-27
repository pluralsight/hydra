package hydra.ingest.modules

import cats.effect._
import cats.syntax.all._
import hydra.avro.util.SchemaWrapper
import hydra.ingest.app.AppConfig.AppConfig
import hydra.ingest.programs.TopicDeletionProgram
import hydra.ingest.services.{IngestionFlow, IngestionFlowV2}
import hydra.kafka.programs.{CreateTopicProgram, KeyAndValueSchemaV2Validator}
import org.typelevel.log4cats.Logger
import retry.RetryPolicies._
import retry.RetryPolicy
import scalacache.{Cache, Mode}
import scalacache.guava.GuavaCache

final class Programs[F[_]: Logger: Sync: Timer: Mode: Concurrent] private(
    cfg: AppConfig,
    algebras: Algebras[F]
) {

  val retryPolicy: RetryPolicy[F] =
    limitRetries[F](cfg.createTopicConfig.numRetries) |+| exponentialBackoff[F](
      cfg.createTopicConfig.baseBackoffDelay
    )

  val createTopic: CreateTopicProgram[F] = CreateTopicProgram.make(
    algebras.schemaRegistry,
    algebras.kafkaAdmin,
    algebras.kafkaClient,
    retryPolicy,
    cfg.metadataTopicsConfig.topicNameV2,
    algebras.metadata,
    cfg.createTopicConfig.defaultLoopHoleCutoffDate
  )

  val ingestionFlow: IngestionFlow[F] = new IngestionFlow[F](
    algebras.schemaRegistry,
    algebras.kafkaClient,
    cfg.createTopicConfig.schemaRegistryConfig.fullUrl
  )

  implicit val guavaCache: Cache[SchemaWrapper] = GuavaCache[SchemaWrapper]

  val ingestionFlowV2: IngestionFlowV2[F] = new IngestionFlowV2[F](
    algebras.schemaRegistry,
    algebras.kafkaClient,
    cfg.createTopicConfig.schemaRegistryConfig.fullUrl,
    algebras.metadata
  )

  val topicDeletion: TopicDeletionProgram[F] = new TopicDeletionProgram[F](
    algebras.kafkaAdmin,
    algebras.kafkaClient,
    cfg.metadataTopicsConfig.topicNameV2,
    cfg.metadataTopicsConfig.topicNameV1,
    algebras.schemaRegistry,
    algebras.metadata,
    algebras.consumerGroups,
    cfg.ignoreDeletionConsumerGroups.consumerGroupListToIgnore,
    cfg.allowableTopicDeletionTimeConfig.allowableTopicDeletionTime
  )

}

object Programs {

  def make[F[_]: Logger: Sync: Timer: Mode: Concurrent](
      appConfig: AppConfig,
      algebras: Algebras[F]
  ): F[Programs[F]] = Sync[F].delay {
    new Programs[F](appConfig, algebras)
  }

}
