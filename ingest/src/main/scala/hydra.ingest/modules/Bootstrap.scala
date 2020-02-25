package hydra.ingest.modules

import java.time.Instant

import cats.Applicative
import cats.data.NonEmptyList
import cats.effect.Sync
import cats.implicits._
import hydra.core.marshallers.History
import hydra.ingest.app.AppConfig.V2MetadataTopicConfig
import hydra.kafka.model._
import hydra.kafka.programs.CreateTopicProgram
import hydra.kafka.util.KafkaUtils.TopicDetails

final class Bootstrap[F[_]: Applicative] private(
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
        TopicMetadataV2Request(
          cfg.topicName,
          Schemas(TopicMetadataV2Key.schema, TopicMetadataV2Value.schema),
          History,
          false,
          InternalUseOnly,
          NonEmptyList.of(cfg.contactMethod),
          Instant.now,
          List.empty,
          Some(
            "This is the topic that Hydra uses to keep track of metadata for topics.")
        ),
        TopicDetails(cfg.numPartitions, cfg.replicationFactor)
      )
    } else {
      Applicative[F].unit
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
