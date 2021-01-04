package hydra.ingest.modules

import java.time.Instant

import cats.data.NonEmptyList
import cats.effect.Sync
import cats.syntax.all._
import cats.{Monad, MonadError}
import hydra.core.marshallers.History
import hydra.ingest.app.AppConfig.{ConsumerOffsetsOffsetsTopicConfig, DVSConsumersTopicConfig, V2MetadataTopicConfig}
import hydra.kafka.model._
import hydra.kafka.programs.CreateTopicProgram
import hydra.kafka.util.KafkaUtils.TopicDetails

final class Bootstrap[F[_]: MonadError[*[_], Throwable]] private (
    createTopicProgram: CreateTopicProgram[F],
    cfg: V2MetadataTopicConfig,
    dvsConsumersTopicConfig: DVSConsumersTopicConfig,
    cooTopicConfig: ConsumerOffsetsOffsetsTopicConfig
) {

  def bootstrapAll: F[Unit] =
    for {
      _ <- bootstrapMetadataTopic
      _ <- bootstrapDVSConsumersTopic
      _ <- bootstrapConsumerOffsetsOffsetsTopic
    } yield ()

  private def bootstrapMetadataTopic: F[Unit] =
    if (cfg.createOnStartup) {
      TopicMetadataV2.getSchemas[F].flatMap { schemas =>
        createTopicProgram.createTopic(
          cfg.topicName,
          TopicMetadataV2Request(
            schemas,
            StreamTypeV2.Entity,
            deprecated = false,
            None,
            InternalUseOnly,
            NonEmptyList.of(cfg.contactMethod),
            Instant.now,
            List.empty,
            Some(
              "This is the topic that Hydra uses to keep track of metadata for topics."
            ),
            Some("Data-Platform")
          ),
          TopicDetails(cfg.numPartitions, cfg.replicationFactor, Map("cleanup.policy" -> "compact"))
        )
      }
    } else {
      Monad[F].unit
    }

  private def bootstrapDVSConsumersTopic: F[Unit] = {
    TopicConsumer.getSchemas[F].flatMap { schemas =>
      createTopicProgram.createTopic(
        dvsConsumersTopicConfig.topicName,
        TopicMetadataV2Request(
          schemas,
          StreamTypeV2.Entity,
          deprecated = false,
          None,
          InternalUseOnly,
          NonEmptyList.of(dvsConsumersTopicConfig.contactMethod),
          Instant.now,
          List.empty,
          Some(
            "This is the topic that Hydra uses to keep track of a summarized list (no partition/offset info) of consumer groups."
          ),
          Some("Data-Platform")
        ),
        TopicDetails(dvsConsumersTopicConfig.numPartitions, dvsConsumersTopicConfig.replicationFactor, Map("cleanup.policy" -> "compact"))
      )
    }
  }

  private def bootstrapConsumerOffsetsOffsetsTopic: F[Unit] =
    TopicConsumerOffset.getSchemas[F].flatMap { schemas =>
      createTopicProgram.createTopic(
        cooTopicConfig.topicName,
        TopicMetadataV2Request(
          schemas,
          StreamTypeV2.Entity,
          deprecated = false,
          None,
          InternalUseOnly,
          NonEmptyList.of(cooTopicConfig.contactMethod),
          Instant.now,
          List.empty,
          Some(
            "This is the topic that Hydra uses to keep track of the offsets we've consumed in the __consumer_offsets topic that Kakfa manages."
          ),
          Some("Data-Platform")
        ),
        TopicDetails(cooTopicConfig.numPartitions, cooTopicConfig.replicationFactor, Map("cleanup.policy" -> "compact"))
      )
    }


}

object Bootstrap {

  def make[F[_]: Sync](
      createTopicProgram: CreateTopicProgram[F],
      v2MetadataTopicConfig: V2MetadataTopicConfig,
      consumersTopicConfig: DVSConsumersTopicConfig,
      consumerOffsetsOffsetsTopicConfig: ConsumerOffsetsOffsetsTopicConfig
  ): F[Bootstrap[F]] = Sync[F].delay {
    new Bootstrap[F](createTopicProgram, v2MetadataTopicConfig, consumersTopicConfig, consumerOffsetsOffsetsTopicConfig)
  }
}
