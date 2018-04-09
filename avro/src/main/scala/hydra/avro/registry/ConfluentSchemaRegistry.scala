package hydra.avro.registry

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.typesafe.config.{Config, ConfigFactory}
import hydra.common.logging.LoggingAdapter
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, MockSchemaRegistryClient, SchemaMetadata, SchemaRegistryClient}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scalacache.guava.GuavaCache

/**
  * Created by alexsilva on 7/6/17.
  */
case class ConfluentSchemaRegistry(registryClient: SchemaRegistryClient, registryUrl: String)
  extends SchemaRegistryComponent {

  def getAllSubjects()(implicit ec: ExecutionContext): Future[Seq[String]] =
    Future(registryClient.getAllSubjects().asScala.map(s => if (s.endsWith("-value")) s.dropRight(6) else s).toSeq)


  def getById(id: Int, suffix: String = "-value")(implicit ec: ExecutionContext): Future[SchemaMetadata] = Future {
    val schema = registryClient.getByID(id)
    val subject = schema.getFullName + suffix
    registryClient.getLatestSchemaMetadata(subject)
  }
}

object ConfluentSchemaRegistry extends LoggingAdapter {

  import configs.syntax._

  private val cachedClients = CacheBuilder.newBuilder()
    .build(
      new CacheLoader[String, ConfluentSchemaRegistry] {
        def load(url: String): ConfluentSchemaRegistry = {
          log.debug(s"Creating new schema registry client for $url")
          val client = if (url == "mock") {
            mockRegistry
          } else {
            new CachedSchemaRegistryClient(url, 1000)
          }
          ConfluentSchemaRegistry(client, url)
        }
      }
    )

  private[this] implicit val guavaCache = GuavaCache[ConfluentSchemaRegistry]

  val mockRegistry = new MockSchemaRegistryClient()

  def registryUrl(config: Config) = config.get[String]("schema.registry.url")
    .valueOrThrow(_ => new IllegalArgumentException("A schema registry url is required."))

  def forConfig(config: Config): ConfluentSchemaRegistry = {
    forConfig("", config)
  }

  def forConfig(path: String, config: Config = ConfigFactory.load()): ConfluentSchemaRegistry = {
    val usedConfig = if (path.isEmpty) config else config.getConfig(path)
    cachedClients.get(registryUrl(usedConfig))
  }
}


