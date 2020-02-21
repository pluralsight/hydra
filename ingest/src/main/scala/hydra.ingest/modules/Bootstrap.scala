package hydra.ingest.modules

import cats.effect.Sync
import hydra.core.bootstrap.CreateTopicProgram
import cats.implicits._
import hydra.ingest.app.AppConfig.V2MetadataTopicConfig

final class Bootstrap[F[_]: Sync] private (
    createTopicProgram: CreateTopicProgram[F],
    cfg: V2MetadataTopicConfig
) {

  def bootstrapAll: F[Unit] =
    for {
      _ <- bootstrapMetadataTopic
    } yield ()

  private def bootstrapMetadataTopic: F[Unit] =
    if (cfg.createOnStartup) {
      createTopicProgram.createTopic(
        cfg.topicName.value,
        cfg.keySchema,
        cfg.valueSchema
      )
    } else {
      Sync[F].unit
    }

}

object Bootstrap {

  def make[F[_]: Sync](
      createTopicProgram: CreateTopicProgram[F],
      v2MetadataTopicConfig: V2MetadataTopicConfig
  ): F[Bootstrap[F]] = Sync[F].delay {
    new Bootstrap[F](createTopicProgram, v2MetadataTopicConfig)
  }
}
