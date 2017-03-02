package hydra.core.avro.registry

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient

/**
  * Created by alexsilva on 2/21/17.
  */
trait SchemaRegistryComponent {
  val registry: SchemaRegistryClient
}
