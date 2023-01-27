package hydra.ingest.modules

import cats.effect.{Async, ConcurrentEffect, ContextShift, Timer}
import cats.syntax.all._
import hydra.avro.registry.SchemaRegistry
import hydra.common.alerting.sender.{InternalNotificationSender, NotificationSender}
import hydra.common.util.Futurable
import hydra.core.http.security.{AccessControlService, AwsIamClient, AwsIamService, AwsSecurityService, AwsStsClient}
import hydra.ingest.app.AppConfig.AppConfig
import hydra.kafka.algebras._
import org.typelevel.log4cats.Logger

final class Algebras[F[_]] private (
    val schemaRegistry: SchemaRegistry[F],
    val kafkaAdmin: KafkaAdminAlgebra[F],
    val kafkaClient: KafkaClientAlgebra[F],
    val metadata: MetadataAlgebra[F],
    val consumerGroups: ConsumerGroupsAlgebra[F],
    val tagsAlgebra: TagsAlgebra[F],
    val accessControlService: AccessControlService[F],
    val awsSecurityService: AwsSecurityService[F]
)

object Algebras {

  def make[F[_]: Async: ConcurrentEffect: ContextShift: Timer: Logger: Futurable](config: AppConfig, internalNotificationsService: InternalNotificationSender[F]): F[Algebras[F]] = {
    implicit val internalNotificationsServiceImpl: InternalNotificationSender[F] = internalNotificationsService
    val schemaRegistryUrl = config.createTopicConfig.schemaRegistryConfig.fullUrl
    for {
      schemaRegistry <- SchemaRegistry.live[F](
        schemaRegistryUrl,
        config.createTopicConfig.schemaRegistryConfig.maxCacheSize,
        config.schemaRegistrySecurityConfig
      )
      kafkaAdmin <- KafkaAdminAlgebra.live[F](config.createTopicConfig.bootstrapServers, kafkaClientSecurityConfig = config.kafkaClientSecurityConfig)
      kafkaClient <- KafkaClientAlgebra.live[F](config.createTopicConfig.bootstrapServers, schemaRegistryUrl,
        schemaRegistry, config.kafkaClientSecurityConfig, config.ingestConfig.recordSizeLimitBytes)
      metadata <- MetadataAlgebra.make[F](config.metadataTopicsConfig.topicNameV2,
        config.metadataTopicsConfig.consumerGroup, kafkaClient, schemaRegistry, config.metadataTopicsConfig.createV2OnStartup)
      consumerGroups <- ConsumerGroupsAlgebra.make[F](
        kafkaInternalTopic = config.consumerGroupsAlgebraConfig.kafkaInternalConsumerGroupsTopic,
        dvsConsumersTopic = config.dvsConsumersTopicConfig.topicName,
        consumerOffsetsOffsetsTopicConfig = config.consumerOffsetsOffsetsTopicConfig.topicName,
        bootstrapServers = config.createTopicConfig.bootstrapServers,
        uniquePerNodeConsumerGroup = config.metadataTopicsConfig.consumerGroup,
        commonConsumerGroup = config.consumerGroupsAlgebraConfig.commonConsumerGroup,
        kafkaClientAlgebra = kafkaClient,
        kAA = kafkaAdmin,
        sra = schemaRegistry,
        config.kafkaClientSecurityConfig
        )
      awsIamClient <- AwsIamClient.make
      awsStsClient <- AwsStsClient.make
      awsIamService <- AwsIamService.make(awsIamClient, awsStsClient)
      awsSecurityService <- AwsSecurityService.make(awsIamService, config.awsConfig)
      accessControlService = new AccessControlService[F](awsSecurityService, config.awsConfig)
      tagsAlgebra <- TagsAlgebra.make(config.tagsConfig.tagsTopic,config.tagsConfig.tagsConsumerGroup,kafkaClient)
    } yield new Algebras[F](schemaRegistry, kafkaAdmin, kafkaClient, metadata, consumerGroups, tagsAlgebra, accessControlService, awsSecurityService)
  }
}
