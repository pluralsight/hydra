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
      defaultReplicationFactor: Short
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
      env("HYDRA_REPLICATION_FACTOR").as[Short].default(3)
    ).parMapN(CreateTopicConfig)

  private implicit val subjectConfigDecoder: ConfigDecoder[String, Subject] =
    ConfigDecoder.identity[String].mapOption("Subject")(Subject.createValidated)

  final case class V2MetadataTopicConfig(
      topicName: Subject,
      createOnStartup: Boolean,
      createV2TopicsEnabled: Boolean,
      contactMethod: ContactMethod,
      numPartitions: Int,
      replicationFactor: Short,
      consumerGroup: ConsumerGroup
  )

  private implicit def contactMethodDecoder
      : ConfigDecoder[String, ContactMethod] =
    ConfigDecoder
      .identity[String]
      .mapOption("ContactMethod")(ContactMethod.create)

  private val v2MetadataTopicConfig: ConfigValue[V2MetadataTopicConfig] =
    (
      env("HYDRA_V2_METADATA_TOPIC_NAME")
        .as[Subject]
        .default(Subject.createValidated("_hydra.v2.metadata").get),
      env("HYDRA_V2_METADATA_CREATE_ON_STARTUP").as[Boolean].default(false),
      env("HYDRA_V2_CREATE_TOPICS_ENABLED").as[Boolean].default(false),
      env(
        "HYDRA_V2_METADATA_CONTACT"
      ).as[ContactMethod],
      env("HYDRA_DEFAULT_PARTITIONS").as[Int].default(10),
      env("HYDRA_REPLICATION_FACTOR").as[Short].default(3),
      env("HYDRA_V2_METADATA_CONSUMER_GROUP")
    ).parMapN(V2MetadataTopicConfig)

  final case class DVSConsumersTopicConfig(
                                            topicName: Subject,
                                            contactMethod: ContactMethod,
                                            numPartitions: Int,
                                            replicationFactor: Short
                                          )

  private val dvsConsumersTopicConfig: ConfigValue[DVSConsumersTopicConfig] =
    (
      env("HYDRA_DVS_CONSUMERS_TOPIC_NAME")
        .as[Subject]
        .default(Subject.createValidated("_hydra.consumer-groups").get),
      env("HYDRA_V2_METADATA_CONTACT").as[ContactMethod],
      env("HYDRA_DEFAULT_PARTITIONS").as[Int].default(10),
      env("HYDRA_REPLICATION_FACTOR").as[Short].default(3)
      ).parMapN(DVSConsumersTopicConfig)

  final case class ConsumerOffsetsOffsetsTopicConfig(
                                                      topicName: Subject,
                                                      contactMethod: ContactMethod,
                                                      numPartitions: Int,
                                                      replicationFactor: Short
                                                    )

  private val consumerOffsetsOffsetsTopicConfig: ConfigValue[ConsumerOffsetsOffsetsTopicConfig] =
    (
      env("HYDRA_CONSUMER_OFFSETS_OFFSETS_TOPIC_NAME")
        .as[Subject]
        .default(Subject.createValidated("_hydra.consumer-offsets-offsets").get),
      env("HYDRA_V2_METADATA_CONTACT").as[ContactMethod],
      env("HYDRA_DEFAULT_PARTITIONS").as[Int].default(10),
      env("HYDRA_REPLICATION_FACTOR").as[Short].default(3)
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

  final case class AppConfig(
      createTopicConfig: CreateTopicConfig,
      v2MetadataTopicConfig: V2MetadataTopicConfig,
      ingestConfig: IngestConfig,
      topicDeletionConfig: TopicDeletionConfig,
      dvsConsumersTopicConfig: DVSConsumersTopicConfig,
      consumerOffsetsOffsetsTopicConfig: ConsumerOffsetsOffsetsTopicConfig,
      consumerGroupsAlgebraConfig: ConsumerGroupsAlgebraConfig
                            )

  val appConfig: ConfigValue[AppConfig] =
    (
      createTopicConfig,
      v2MetadataTopicConfig,
      ingestConfig,
      topicDeletionConfig,
      dvsConsumersTopicConfig,
      consumerOffsetsOffsetsTopicConfig,
      consumerGroupAlgebraConfig
    ).parMapN(AppConfig)
}
