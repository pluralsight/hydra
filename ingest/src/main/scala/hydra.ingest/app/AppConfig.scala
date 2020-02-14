package hydra.ingest.app

import java.net.URL
import ciris.ConfigValue
import cats.implicits._
import ciris._
import cats.effect.IO
import scala.concurrent.duration._

object AppConfig {

  final case class AppConfig(
    createTopicConfig: CreateTopicConfig
  )

  final case class SchemaRegistryConfig(
    fullUrl: String,
    maxCacheSize: Int
  )

  final case class CreateTopicConfig(
    schemaRegistryConfig: SchemaRegistryConfig,
    numRetries: Int,
    baseBackoffDelay: FiniteDuration
  )

  private val schemaRegistryConfig: ConfigValue[SchemaRegistryConfig] =
    (
      env("HYDRA_SCHEMA_REGISTRY_URL").as[String].default("http://localhost:8081"),
      env("HYDRA_MAX_SCHEMAS_PER_SUBJECT").as[Int].default(1000)
    )
    .parMapN(SchemaRegistryConfig)

  private val createTopicConfig: ConfigValue[CreateTopicConfig] =
    (
      schemaRegistryConfig,
      env("CREATE_TOPIC_NUM_RETRIES").as[Int].default(1),
      env("CREATE_TOPIC_BASE_BACKOFF_DELAY").as[FiniteDuration].default(1.second) 
    )
    .parMapN(CreateTopicConfig)

  val appConfig: ConfigValue[AppConfig] =
    (
      createTopicConfig
    )
    .map(AppConfig)

}