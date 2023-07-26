package hydra.ingest.app

import cats.syntax.all._
import ciris.{ConfigDecoder, ConfigValue, env}
import hydra.common.config.KafkaConfigUtils.{KafkaClientSecurityConfig, SchemaRegistrySecurityConfig, kafkaClientSecurityConfig, schemaRegistrySecurityConfig}
import eu.timepit.refined.types.string.NonEmptyString
import hydra.core.http.security.entity.AwsConfig
import hydra.kafka.algebras.KafkaClientAlgebra.ConsumerGroup
import hydra.kafka.model.ContactMethod
import hydra.kafka.model.TopicMetadataV2Request.Subject

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, ZoneId}
import scala.concurrent.duration._

object AppConfig {

  final case class SchemaRegistryConfig(
      fullUrl: String,
      maxCacheSize: Int,
      schemaRegistryClientRetriesConfig: Int,
      schemaRegistryClientRetrieDelaysConfig: FiniteDuration
      )

  final case class SchemaRegistryRedisConfig(
     redisUrl: String,
     redisPort: Int,
     idCacheTtl: Int = 1,
     schemaCacheTtl: Int = 1,
     versionCacheTtl: Int = 1,
     useRedisClient: Boolean = false
  )

  private val schemaRegistryConfig: ConfigValue[SchemaRegistryConfig] =
    (
      env("HYDRA_SCHEMA_REGISTRY_URL")
        .as[String]
        .default("http://localhost:8081"),
      env("HYDRA_MAX_SCHEMAS_PER_SUBJECT").as[Int].default(1000),
      env("HYDRA_SCHEMA_REGISTRY_RETRIES").as[Int].default(3),
      env("HYDRA_SCHEMA_REGISTRY_RETRIES_DELAY").as[FiniteDuration].default(500.milliseconds)
      ).parMapN(SchemaRegistryConfig)

  private val schemaRegistryRedisConfig: ConfigValue[SchemaRegistryRedisConfig] = (
    env("HYDRA_SCHEMA_REGISTRY_REDIS_HOST")
      .as[String]
      .default("localhost"),
    env("HYDRA_SCHEMA_REGISTRY_REDIS_PORT")
      .as[Int]
      .default(6379),
    env("HYDRA_SCHEMA_REGISTRY_REDIS_ID_CACHE_TTL")
      .as[Int]
      .default(1),
    env("HYDRA_SCHEMA_REGISTRY_REDIS_SCHEMA_CACHE_TTL")
      .as[Int]
      .default(1),
    env("HYDRA_SCHEMA_REGISTRY_REDIS_VERSION_CACHE_TTL")
      .as[Int]
      .default(1),
    env("HYDRA_SCHEMA_REGISTRY_USE_REDIS")
      .as[Boolean]
      .default(false)
    ).parMapN(SchemaRegistryRedisConfig)

  final case class CreateTopicConfig(
      schemaRegistryConfig: SchemaRegistryConfig,
      schemaRegistryRedisConfig: SchemaRegistryRedisConfig,
      numRetries: Int,
      baseBackoffDelay: FiniteDuration,
      bootstrapServers: String,
      defaultNumPartions: Int,
      defaultReplicationFactor: Short,
      defaultMinInsyncReplicas: Short,
      defaultLoopHoleCutoffDate: Instant
  )

  private[app] implicit val dateStringToInstantDecoder: ConfigDecoder[String, Instant] =
    ConfigDecoder.identity[String].mapOption("Instant")(date =>
      Some(LocalDate
        .parse(date, DateTimeFormatter.BASIC_ISO_DATE)
        .atStartOfDay(ZoneId.of("UTC"))
        .toInstant)
    )

  private val createTopicConfig: ConfigValue[CreateTopicConfig] =
    (
      schemaRegistryConfig,
      schemaRegistryRedisConfig,
      env("CREATE_TOPIC_NUM_RETRIES").as[Int].default(1),
      env("CREATE_TOPIC_BASE_BACKOFF_DELAY")
        .as[FiniteDuration]
        .default(1.second),
      env("HYDRA_KAFKA_PRODUCER_BOOTSTRAP_SERVERS").as[String],
      env("HYDRA_DEFAULT_PARTIONS").as[Int].default(10),
      env("HYDRA_REPLICATION_FACTOR").as[Short].default(3),
      env("HYDRA_MIN_INSYNC_REPLICAS").as[Short].default(2),
      env("DEFAULT_LOOPHOLE_CUTOFF_DATE_IN_YYYYMMDD")
        .as[Instant]
        .default(Instant.parse("2023-08-31T00:00:00Z"))
      ).parMapN(CreateTopicConfig)

  private implicit val subjectConfigDecoder: ConfigDecoder[String, Subject] =
    ConfigDecoder.identity[String].mapOption("Subject")(Subject.createValidated)

    final case class NotificationsConfig(
                                          notificationUri: String,
                                          internalSlackNotificationsChannel: String,
                                          internalSlackNotificationUrl: Option[NonEmptyString]
                                        )

    private val notificationsConfig: ConfigValue[NotificationsConfig] = (
      env("HYDRA_NOTIFICATION_URL").as[String].default("http://localhost:8080"),
      env("HYDRA_SYSTEM_NOTIFICATION_SLACK_CHANNEL")
        .as[String]
        .default("test-messages-thread")
      ).parMapN {
      case (notificationUrl, internalNotificationSlackChannel) =>
        NotificationsConfig(
          notificationUrl,
          internalNotificationSlackChannel,
          NonEmptyString.from(s"$notificationUrl/notify/slack?channel=$internalNotificationSlackChannel").toOption
        )
    }

  final case class IgnoreDeletionConsumerGroups(consumerGroupListToIgnore: List[String])

  private val ignoreDeletionConsumerGroups: ConfigValue[IgnoreDeletionConsumerGroups] = (
    env("HYDRA_IGNORE_DELETION_CONSUMER_GROUP").as[String].default("")
    ).map(cfg => IgnoreDeletionConsumerGroups(cfg.split(",").toList))

  final case class MetadataTopicsConfig(
      topicNameV1: Subject,
      topicNameV2: Subject,
      createV1OnStartup: Boolean,
      createV2OnStartup: Boolean,
      createV2TopicsEnabled: Boolean,
      contactMethod: ContactMethod,
      numPartitions: Int,
      replicationFactor: Short,
      minInsyncReplicas: Short,
      consumerGroup: ConsumerGroup
  )

  private implicit def contactMethodDecoder
      : ConfigDecoder[String, ContactMethod] =
    ConfigDecoder
      .identity[String]
      .mapOption("ContactMethod")(ContactMethod.create)

  private val metadataTopicsConfig: ConfigValue[MetadataTopicsConfig] =
    (
      env("HYDRA_V1_METADATA_TOPIC_NAME")
        .as[Subject]
        .default(Subject.createValidated("_hydra.metadata.topic").get),
      env("HYDRA_V2_METADATA_TOPIC_NAME")
        .as[Subject]
        .default(Subject.createValidated("_hydra.v2.metadata").get),
      env("HYDRA_V1_METADATA_CREATE_ON_STARTUP").as[Boolean].default(true),
      env("HYDRA_V2_METADATA_CREATE_ON_STARTUP").as[Boolean].default(false),
      env("HYDRA_V2_CREATE_TOPICS_ENABLED").as[Boolean].default(false),
      env(
        "HYDRA_V2_METADATA_CONTACT"
      ).as[ContactMethod],
      env("HYDRA_DEFAULT_PARTITIONS").as[Int].default(10),
      env("HYDRA_REPLICATION_FACTOR").as[Short].default(3),
      env("HYDRA_MIN_INSYNC_REPLICAS").as[Short].default(2),
      env("HYDRA_V2_METADATA_CONSUMER_GROUP")
    ).parMapN(MetadataTopicsConfig)

  final case class DVSConsumersTopicConfig(
                                            topicName: Subject,
                                            contactMethod: ContactMethod,
                                            numPartitions: Int,
                                            replicationFactor: Short,
                                            minInsyncReplicas: Short
                                          )

  private val dvsConsumersTopicConfig: ConfigValue[DVSConsumersTopicConfig] =
    (
      env("HYDRA_DVS_CONSUMERS_TOPIC_NAME")
        .as[Subject]
        .default(Subject.createValidated("_hydra.consumer-groups").get),
      env("HYDRA_V2_METADATA_CONTACT").as[ContactMethod],
      env("HYDRA_DEFAULT_PARTITIONS").as[Int].default(10),
      env("HYDRA_REPLICATION_FACTOR").as[Short].default(3),
      env("HYDRA_MIN_INSYNC_REPLICAS").as[Short].default(2),
      ).parMapN(DVSConsumersTopicConfig)

  final case class ConsumerOffsetsOffsetsTopicConfig(
                                                      topicName: Subject,
                                                      contactMethod: ContactMethod,
                                                      numPartitions: Int,
                                                      replicationFactor: Short,
                                                      minInsyncReplicas: Short
                                                    )

  private val consumerOffsetsOffsetsTopicConfig: ConfigValue[ConsumerOffsetsOffsetsTopicConfig] =
    (
      env("HYDRA_CONSUMER_OFFSETS_OFFSETS_TOPIC_NAME")
        .as[Subject]
        .default(Subject.createValidated("_hydra.consumer-offsets-offsets").get),
      env("HYDRA_V2_METADATA_CONTACT").as[ContactMethod],
      env("HYDRA_DEFAULT_PARTITIONS").as[Int].default(10),
      env("HYDRA_REPLICATION_FACTOR").as[Short].default(3),
      env("HYDRA_MIN_INSYNC_REPLICAS").as[Short].default(2),

      ).parMapN(ConsumerOffsetsOffsetsTopicConfig)

  final case class ConsumerGroupsAlgebraConfig(
                                                      kafkaInternalConsumerGroupsTopic: String,
                                                      commonConsumerGroup: ConsumerGroup,
                                                      consumerGroupsConsumerEnabled: Boolean
                                                    )

  private val consumerGroupAlgebraConfig: ConfigValue[ConsumerGroupsAlgebraConfig] =
      (
        env("KAFKA_CONSUMER_GROUPS_INTERNAL_TOPIC_NAME").as[String].default("__consumer_offsets"),
          env("HYDRA_CONSUMER_GROUPS_COMMON_CONSUMER_GROUP").as[ConsumerGroup].default("kafkaInternalConsumerGroupsTopic-ConsumerGroupName"),
            env("CONSUMER_GROUPS_CONSUMER_ENABLED").as[Boolean].default(true)
        ).parMapN(ConsumerGroupsAlgebraConfig)

  final case class IngestConfig(
                                 recordSizeLimitBytes: Option[Long]
                               )

  private[app] implicit def decodeSetStrings
  : ConfigDecoder[String, Set[String]] =
    ConfigDecoder
      .identity[String]
      .mapOption("Set[String]")(s => Some(if (s.isEmpty) Set.empty else s.split(",").toSet))

  private val ingestConfig: ConfigValue[IngestConfig] =
    (
      env("HYDRA_INGEST_RECORD_SIZE_LIMIT_BYTES").as[Long].option
      ).map(IngestConfig)

  final case class TopicDeletionConfig(deleteTopicPassword: String)

  private val topicDeletionConfig: ConfigValue[TopicDeletionConfig] =
    (
      env("HYDRA_INGEST_TOPIC_DELETION_PASSWORD").as[String].default("")
      ).map(TopicDeletionConfig)

  private val awsConfig: ConfigValue[AwsConfig] =
    (
    env("MSK_CLUSTER_ARN").as[String].option,
    env("AWS_IAM_SECURITY_ENABLED").as[Boolean].default(false)
    ).mapN(AwsConfig)

  final case class TagsConfig(tagsPassword: String, tagsTopic: String, tagsConsumerGroup: String)

  private val tagsConfig: ConfigValue[TagsConfig] =
    (
      env("HYDRA_TAGS_ENDPOINT_PASSWORD").as[String].default(""),
      env("HYDRA_TAGS_TOPIC").as[String].default("_hydra.tags-topic"),
      env("HYDRA_TAGS_CONSUMER_GROUP").as[String].default("_hydra.tags-consumer-group")
    ).mapN(TagsConfig)

  final case class AllowableTopicDeletionTimeConfig(allowableTopicDeletionTime: Long)

  private val allowableTopicDeletionTimeConfig: ConfigValue[AllowableTopicDeletionTimeConfig] = (
    env("HYDRA_ALLOWABLE_TOPIC_DELETION_TIME_MS").as[Long].default(14400000)
    ).map(AllowableTopicDeletionTimeConfig) // Default 4 hours

  final case class CorsAllowedOriginConfig(corsAllowedOrigins: String)

  private val corsAllowedOrigin: ConfigValue[CorsAllowedOriginConfig] =
    env("CORS_ALLOWED_ORIGIN").as[String].default("*").map(CorsAllowedOriginConfig)

  final case class AppConfig(
                              createTopicConfig: CreateTopicConfig,
                              metadataTopicsConfig: MetadataTopicsConfig,
                              ingestConfig: IngestConfig,
                              topicDeletionConfig: TopicDeletionConfig,
                              tagsConfig: TagsConfig,
                              dvsConsumersTopicConfig: DVSConsumersTopicConfig,
                              consumerOffsetsOffsetsTopicConfig: ConsumerOffsetsOffsetsTopicConfig,
                              consumerGroupsAlgebraConfig: ConsumerGroupsAlgebraConfig,
                              ignoreDeletionConsumerGroups: IgnoreDeletionConsumerGroups,
                              allowableTopicDeletionTimeConfig: AllowableTopicDeletionTimeConfig,
                              corsAllowedOriginConfig: CorsAllowedOriginConfig,
                              kafkaClientSecurityConfig: KafkaClientSecurityConfig,
                              schemaRegistrySecurityConfig: SchemaRegistrySecurityConfig,
                              notificationsConfig: NotificationsConfig,
                              awsConfig: AwsConfig,
                              schemaRegistryRedisConfig: SchemaRegistryRedisConfig
                            )

  val appConfig: ConfigValue[AppConfig] =
    (
      createTopicConfig,
      metadataTopicsConfig,
      ingestConfig,
      topicDeletionConfig,
      tagsConfig,
      dvsConsumersTopicConfig,
      consumerOffsetsOffsetsTopicConfig,
      consumerGroupAlgebraConfig,
      ignoreDeletionConsumerGroups,
      allowableTopicDeletionTimeConfig,
      corsAllowedOrigin,
      kafkaClientSecurityConfig,
      schemaRegistrySecurityConfig,
      notificationsConfig,
      awsConfig,
      schemaRegistryRedisConfig
    ).parMapN(AppConfig)
}
