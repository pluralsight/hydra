package hydra.ingest.modules

import cats.effect._
import cats.implicits._
import hydra.ingest.app.AppConfig.CreateTopicConfig
import hydra.kafka.programs.CreateTopicProgram
import io.chrisdavenport.log4cats.Logger
import retry.RetryPolicies._
import retry.RetryPolicy

final class Programs[F[_]: Logger: Sync: Timer] private (
    cfg: CreateTopicConfig,
    algebras: Algebras[F]
) {

  val retryPolicy: RetryPolicy[F] =
    limitRetries[F](cfg.numRetries) |+| exponentialBackoff[F](
      cfg.baseBackoffDelay
    )

  val createTopic: CreateTopicProgram[F] = new CreateTopicProgram[F](
    algebras.schemaRegistry,
    retryPolicy
  )

}

object Programs {

  def make[F[_]: Logger: Sync: Timer](
      createTopicConfig: CreateTopicConfig,
      algebras: Algebras[F]
  ): F[Programs[F]] = Sync[F].delay {
    new Programs[F](createTopicConfig, algebras)
  }

}
