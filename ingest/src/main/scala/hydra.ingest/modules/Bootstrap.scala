package hydra.ingest.modules

import java.time.Instant

import cats.data.NonEmptyList
import cats.effect.Sync
import cats.syntax.all._
import cats.{Monad, MonadError}
import hydra.core.marshallers.History
import hydra.ingest.app.AppConfig.{ConsumerOffsetsOffsetsTopicConfig, DVSConsumersTopicConfig, MetadataTopicsConfig}
import hydra.kafka.model._
import hydra.kafka.programs.CreateTopicProgram
import hydra.kafka.util.KafkaUtils.TopicDetails
import hydra.kafka.algebras.KafkaAdminAlgebra

final class Bootstrap[F[_]: MonadError[*[_], Throwable]] private (
    createTopicProgram: CreateTopicProgram[F],
    cfg: MetadataTopicsConfig,
    dvsConsumersTopicConfig: DVSConsumersTopicConfig,
    cooTopicConfig: ConsumerOffsetsOffsetsTopicConfig,
    kafkaAdmin: KafkaAdminAlgebra[F]
) {

  def bootstrapAll: F[Unit] =
    for {
      _ <- bootstrapMetadataTopicV2
      _ <- bootstrapMetadataTopicV1
      _ <- bootstrapDVSConsumersTopic
      _ <- bootstrapConsumerOffsetsOffsetsTopic
    } yield ()

  private def bootstrapMetadataTopicV1: F[Unit] = {
    if (cfg.createV1OnStartup) {
      kafkaAdmin.describeTopic(cfg.topicNameV1.value).flatMap {
        case Some(_) => Monad[F].unit
        case None => kafkaAdmin.createTopic(
          cfg.topicNameV1.value,
          TopicDetails(cfg.numPartitions, cfg.replicationFactor, Map("cleanup.policy" -> "compact"))
        )
      }
    } else {
      Monad[F].unit
    }
  }

  private def bootstrapMetadataTopicV2: F[Unit] =
    if (cfg.createV2OnStartup) {
      TopicMetadataV2.getSchemas[F].flatMap { schemas =>
        createTopicProgram.createTopic(
          cfg.topicNameV2,
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
            Some("Data-Platform"),
            None,
            Map("createdBy" -> "DVS")
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
          Some("Data-Platform"),
          None,
          Map("createdBy" -> "DVS")
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
          Some("Data-Platform"),
          None,
          Map("createdBy" -> "DVS")
        ),
        TopicDetails(cooTopicConfig.numPartitions, cooTopicConfig.replicationFactor, Map("cleanup.policy" -> "compact"))
      )
    }


}

object Bootstrap {

  def make[F[_]: Sync](
      createTopicProgram: CreateTopicProgram[F],
      metadataTopicsConfig: MetadataTopicsConfig,
      consumersTopicConfig: DVSConsumersTopicConfig,
      consumerOffsetsOffsetsTopicConfig: ConsumerOffsetsOffsetsTopicConfig,
      kafkaAdmin: KafkaAdminAlgebra[F]
  ): F[Bootstrap[F]] = Sync[F].delay {
    new Bootstrap[F](createTopicProgram, metadataTopicsConfig, consumersTopicConfig, consumerOffsetsOffsetsTopicConfig, kafkaAdmin)
  }
}
