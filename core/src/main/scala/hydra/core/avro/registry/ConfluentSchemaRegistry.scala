package hydra.core.avro.registry

import com.typesafe.config.Config
import configs.syntax._
import hydra.common.config.ConfigSupport
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, MockSchemaRegistryClient, SchemaRegistryClient}

/**
  * Created by alexsilva on 2/21/17.
  */
trait ConfluentSchemaRegistry extends SchemaRegistryComponent with ConfigSupport {
  val registry: SchemaRegistryClient = ConfluentSchemaRegistry.fromConfig(applicationConfig)
  val registryUrl: String = ConfluentSchemaRegistry.registryUrl(applicationConfig)
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

