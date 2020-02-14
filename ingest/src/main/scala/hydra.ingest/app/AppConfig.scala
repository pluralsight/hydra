package hydra.ingest.app

import cats.implicits._
import ciris.{ConfigValue, _}
import hydra.kafka.config.MetadataSchemaConfig
import hydra.kafka.model.Subject
import org.apache.avro.Schema

import scala.concurrent.duration._

object AppConfig {

  final case class SchemaRegistryConfig(
    fullUrl: String,
    maxCacheSize: Int
  )

  private val schemaRegistryConfig: ConfigValue[SchemaRegistryConfig] =
    (
      env("HYDRA_SCHEMA_REGISTRY_URL").as[String].default("http://localhost:8081"),
      env("HYDRA_MAX_SCHEMAS_PER_SUBJECT").as[Int].default(1000)
    )
    .parMapN(SchemaRegistryConfig)

  final case class CreateTopicConfig(
    schemaRegistryConfig: SchemaRegistryConfig,
    numRetries: Int,
    baseBackoffDelay: FiniteDuration
  )

  private val createTopicConfig: ConfigValue[CreateTopicConfig] =
    (
      schemaRegistryConfig,
      env("CREATE_TOPIC_NUM_RETRIES").as[Int].default(1),
      env("CREATE_TOPIC_BASE_BACKOFF_DELAY").as[FiniteDuration].default(1.second)
    )
    .parMapN(CreateTopicConfig)

  private implicit val subjectConfigDecoder: ConfigDecoder[String, Subject] =
    ConfigDecoder[String, String].mapOption("Subject")(Subject.createValidated)

  final case class V2MetadataTopicConfig(
    topicName: Subject,
    keySchema: Schema,
    valueSchema: Schema,
    createOnStartup: Boolean
  )

  private val v2MetadataTopicConfig: ConfigValue[V2MetadataTopicConfig] =
    (
      env("HYDRA_V2_METADATA_TOPIC_NAME").as[Subject].default(Subject.createValidated("_hydra.v2.metadata").get),
      env("HYDRA_V2_METADATA_CREATE_ON_STARTUP").as[Boolean].default(false)
    )
    .parMapN { (subject, createOnStartup) =>
      V2MetadataTopicConfig(subject, MetadataSchemaConfig.keySchema, MetadataSchemaConfig.valueSchema, createOnStartup)
    }

  final case class AppConfig(
    createTopicConfig: CreateTopicConfig,
    v2MetadataTopicConfig: V2MetadataTopicConfig
  )

  val appConfig: ConfigValue[AppConfig] =
    (
      createTopicConfig,
      v2MetadataTopicConfig
    )
    .parMapN(AppConfig)

}