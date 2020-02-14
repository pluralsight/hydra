package hydra.ingest.app

import java.nio.file.Paths

import cats.effect.Blocker
import cats.implicits._
import ciris.{ConfigValue, _}
import hydra.kafka.model.Subject
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser

import scala.concurrent.duration._
import scala.util.Try

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

  private implicit val schemaConfigDecoder: ConfigDecoder[String, Schema] =
    ConfigDecoder[String, String].mapOption("Schema")(s => Try(new Parser().parse(s)).toOption)

  private def schemaFromResource(resourceName: String, blocker: Blocker): ConfigValue[Schema] = {
    file(Paths.get(ClassLoader.getSystemResource(resourceName).toURI), blocker).as[Schema]
  }

  final case class V2MetadataTopicConfig(topicName: Subject, keySchema: Schema, valueSchema: Schema)

  private def v2MetadataTopicConfig(blocker: Blocker): ConfigValue[V2MetadataTopicConfig] =
    (
      env("HYDRA_V2_METADATA_TOPIC_NAME").as[Subject].default(Subject.createValidated("_hydra.v2.metadata").get),
      schemaFromResource("HydraMetadataTopicKeyV2.avsc", blocker),
      schemaFromResource("HydraMetadataTopicValueV2.avsc", blocker)
    )
    .parMapN(V2MetadataTopicConfig)

  final case class AppConfig(
    createTopicConfig: CreateTopicConfig,
    v2MetadataTopicConfig: V2MetadataTopicConfig
  )

  def appConfig(blocker: Blocker): ConfigValue[AppConfig] =
    (
      createTopicConfig,
      v2MetadataTopicConfig(blocker)
    )
    .parMapN(AppConfig)

}