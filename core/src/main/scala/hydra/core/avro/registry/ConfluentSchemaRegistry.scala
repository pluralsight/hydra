package hydra.core.avro.registry

import com.typesafe.config.Config
import configs.syntax._
import hydra.common.config.ConfigSupport
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, MockSchemaRegistryClient, SchemaMetadata, SchemaRegistryClient}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by alexsilva on 2/21/17.
  */
trait ConfluentSchemaRegistry extends SchemaRegistryComponent with ConfigSupport {

  val registry: SchemaRegistryClient = ConfluentSchemaRegistry.fromConfig(applicationConfig)
  val registryUrl: String = ConfluentSchemaRegistry.registryUrl(applicationConfig)


  def getAllSubjects()(implicit ec: ExecutionContext): Future[Seq[String]] =
    Future(registry.getAllSubjects().asScala.map(_.dropRight(6)).toSeq)

  /**
    * Due to limititations of the client registry API, this will only work for schemas registered within Hydra.
    *
    * @param id
    * @return
    */
  def getById(id: Int)(implicit ec: ExecutionContext): Future[SchemaMetadata] = Future {
    val schema = registry.getByID(id)
    val subject = schema.getNamespace + schema.getName + "-value"
    registry.getLatestSchemaMetadata(subject)
  }
}

object ConfluentSchemaRegistry {

  val mockRegistry = new MockSchemaRegistryClient()

  def registryUrl(config: Config) = config.get[String]("schema.registry.url")
    .valueOrThrow(_ => new IllegalArgumentException("A schema registry url is required."))

  def fromConfig(config: Config): SchemaRegistryClient = {
    val url = registryUrl(config)
    val identityMapCapacity = config.get[Int]("schema.registry.map.capacity").valueOrElse(1000)
    if (url == "mock") mockRegistry
    else new CachedSchemaRegistryClient(url, identityMapCapacity)
  }
}

