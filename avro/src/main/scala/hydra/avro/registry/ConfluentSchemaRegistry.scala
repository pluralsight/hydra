package hydra.avro.registry

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.typesafe.config.{Config, ConfigFactory}
import hydra.common.logging.LoggingAdapter
import io.confluent.kafka.schemaregistry.client.{
  CachedSchemaRegistryClient,
  MockSchemaRegistryClient,
  SchemaMetadata,
  SchemaRegistryClient
}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by alexsilva on 7/6/17.
  */
case class ConfluentSchemaRegistry(
    registryClient: SchemaRegistryClient,
    registryUrl: String
) extends SchemaRegistryComponent {

  def getAllSubjects()(implicit ec: ExecutionContext): Future[Seq[String]] =
    Future(
      registryClient
        .getAllSubjects()
        .asScala
        .map(s => if (s.endsWith("-value")) s.dropRight(6) else s)
        .toSeq
    )

  def getById(id: Int, suffix: String = "-value")(
      implicit ec: ExecutionContext
  ): Future[SchemaMetadata] = Future {
    val schema = registryClient.getById(id)
    val subject = schema.getFullName + suffix
    registryClient.getLatestSchemaMetadata(subject)
  }
}

object ConfluentSchemaRegistry extends LoggingAdapter {

  import hydra.common.config.ConfigSupport._

  case class SchemaRegistryClientInfo(
      url: String,
      schemaRegistryMaxCapacity: Int
  )

  private val cachedClients = CacheBuilder
    .newBuilder()
    .build(
      new CacheLoader[SchemaRegistryClientInfo, ConfluentSchemaRegistry] {

        def load(info: SchemaRegistryClientInfo): ConfluentSchemaRegistry = {
          log.debug(s"Creating new schema registry client for ${info.url}")
          val client = if (info.url == "mock") {
            mockRegistry
          } else {
            new CachedSchemaRegistryClient(
              info.url,
              info.schemaRegistryMaxCapacity
            )
          }
          ConfluentSchemaRegistry(client, info.url)
        }
      }
    )

  val mockRegistry = new MockSchemaRegistryClient()

  def registryUrl(config: Config): String =
    config.getStringOpt("schema.registry.url")
      .getOrElse(throw new IllegalArgumentException("A schema registry url is required."))

  def forConfig(
      config: Config = ConfigFactory.load()
  ): ConfluentSchemaRegistry = {
    val identityMapCapacity =
      config.getIntOpt("max.schemas.per.subject").getOrElse(1000)
    cachedClients.get(
      SchemaRegistryClientInfo(registryUrl(config), identityMapCapacity)
    )
  }
}
