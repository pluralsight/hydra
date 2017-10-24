package hydra.avro.registry

import com.typesafe.config.{Config, ConfigFactory}
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, MockSchemaRegistryClient, SchemaMetadata, SchemaRegistryClient}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scalacache.guava.GuavaCache

/**
  * Created by alexsilva on 7/6/17.
  */
class ConfluentSchemaRegistry(val registryClient: SchemaRegistryClient, val registryUrl: String)
  extends SchemaRegistryComponent {

  def getAllSubjects()(implicit ec: ExecutionContext): Future[Seq[String]] =
    Future(registryClient.getAllSubjects().asScala.map(s => if (s.endsWith("-value")) s.dropRight(6) else s).toSeq)


  def getById(id: Int, suffix: String = "-value")(implicit ec: ExecutionContext): Future[SchemaMetadata] = Future {
    val schema = registryClient.getByID(id)
    val subject = schema.getFullName + suffix
    registryClient.getLatestSchemaMetadata(subject)
  }
}

object ConfluentSchemaRegistry {

  import configs.syntax._
  import scalacache._

  private[this] implicit val scalaCache = ScalaCache(GuavaCache())

  val mockRegistry = new MockSchemaRegistryClient()

  def registryUrl(config: Config) = config.get[String]("schema.registry.url")
    .valueOrThrow(_ => new IllegalArgumentException("A schema registry url is required."))

  def forConfig(config: Config): ConfluentSchemaRegistry = {
    forConfig("", config)
  }

  def forConfig(path: String, config: Config = ConfigFactory.load()): ConfluentSchemaRegistry = {
    val usedConfig = if (path.isEmpty) config else config.getConfig(path)
    val url = registryUrl(usedConfig)
    sync.caching(url) {
      val identityMapCapacity = config.get[Int]("schema.registry.map.capacity").valueOrElse(1000)
      val client = if (url == "mock") mockRegistry else new CachedSchemaRegistryClient(url, identityMapCapacity)
      new ConfluentSchemaRegistry(client, url)
    }
  }
}
