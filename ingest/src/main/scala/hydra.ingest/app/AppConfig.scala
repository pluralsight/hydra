package hydra.ingest.app

import cats.syntax.all._
import ciris.{ConfigValue, env, _}
import hydra.kafka.algebras.KafkaClientAlgebra.ConsumerGroup
import hydra.kafka.model.ContactMethod
import hydra.kafka.model.TopicMetadataV2Request.Subject

import scala.concurrent.duration._

object AppConfig {

  final case class SchemaRegistryConfig(
      fullUrl: String,
      maxCacheSize: Int
  )

  private val schemaRegistryConfig: ConfigValue[SchemaRegistryConfig] =
    (
      env("HYDRA_SCHEMA_REGISTRY_URL")
        .as[String]
        .default("http://localhost:8081"),
      env("HYDRA_MAX_SCHEMAS_PER_SUBJECT").as[Int].default(1000)
    ).parMapN(SchemaRegistryConfig)

  final case class CreateTopicConfig(
      schemaRegistryConfig: SchemaRegistryConfig,
      numRetries: Int,
      baseBackoffDelay: FiniteDuration,
      bootstrapServers: String,
      defaultNumPartions: Int,
      defaultReplicationFactor: Short,
      defaultMinInsyncReplicas: Short
  )

  private val createTopicConfig: ConfigValue[CreateTopicConfig] =
    (
      schemaRegistryConfig,
      env("CREATE_TOPIC_NUM_RETRIES").as[Int].default(1),
      env("CREATE_TOPIC_BASE_BACKOFF_DELAY")
        .as[FiniteDuration]
        .default(1.second),
      env("HYDRA_KAFKA_PRODUCER_BOOTSTRAP_SERVERS").as[String],
      env("HYDRA_DEFAULT_PARTIONS").as[Int].default(10),
      env("HYDRA_REPLICATION_FACTOR").as[Short].default(3),
      env("HYDRA_MIN_INSYNC_REPLICAS").as[Short].default(2)
    ).parMapN(CreateTopicConfig)

  private implicit val subjectConfigDecoder: ConfigDecoder[String, Subject] =
    ConfigDecoder.identity[String].mapOption("Subject")(Subject.createValidated)

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

  final case class TagsConfig(tagsPassword: String, tagsTopic: String, tagsConsumerGroup: String)

  private val tagsConfig: ConfigValue[TagsConfig] =
    (
      env("TAGS_ENDPOINT_PASSWORD").as[String].default(""),
      env("TAGS_TOPIC").as[String].default("_hydra.tags-topic"),
      env("TAGS_CONSUMER_GROUP").as[String].default("_hydra.tags-consumer-group")
    ).mapN(TagsConfig)

  final case class AppConfig(
                              createTopicConfig: CreateTopicConfig,
                              metadataTopicsConfig: MetadataTopicsConfig,
                              ingestConfig: IngestConfig,
                              topicDeletionConfig: TopicDeletionConfig,
                              tagsConfig: TagsConfig,
                              dvsConsumersTopicConfig: DVSConsumersTopicConfig,
                              consumerOffsetsOffsetsTopicConfig: ConsumerOffsetsOffsetsTopicConfig,
                              consumerGroupsAlgebraConfig: ConsumerGroupsAlgebraConfig
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
      consumerGroupAlgebraConfig
    ).parMapN(AppConfig)
}
