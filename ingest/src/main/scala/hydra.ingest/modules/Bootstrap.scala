package hydra.ingest.modules

import java.time.Instant

import cats.data.NonEmptyList
import cats.effect.Sync
import cats.implicits._
import cats.{Monad, MonadError}
import hydra.core.marshallers.History
import hydra.ingest.app.AppConfig.V2MetadataTopicConfig
import hydra.kafka.model._
import hydra.kafka.programs.CreateTopicProgram
import hydra.kafka.util.KafkaUtils.TopicDetails

final class Bootstrap[F[_]: MonadError[*[_], Throwable]] private (
    createTopicProgram: CreateTopicProgram[F],
    cfg: V2MetadataTopicConfig
) {

  def bootstrapAll: F[Unit] =
    for {
      _ <- bootstrapMetadataTopic
    } yield ()

  //TODO create the summarizedConsumerGroups Topic
  //TODO create consumerGroupOffsetTopic (look into storing this value in Zookeeper)

  private def bootstrapMetadataTopic: F[Unit] =
    if (cfg.createOnStartup) {
      TopicMetadataV2.getSchemas[F].flatMap { schemas =>
        createTopicProgram.createTopic(
          cfg.topicName,
          TopicMetadataV2Request(
            schemas,
            StreamTypeV2.Entity,
            false,
            InternalUseOnly,
            NonEmptyList.of(cfg.contactMethod),
            Instant.now,
            List.empty,
            Some(
              "This is the topic that Hydra uses to keep track of metadata for topics."
            )
          ),
          TopicDetails(cfg.numPartitions, cfg.replicationFactor)
        )
      }
    } else {
      Monad[F].unit
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
