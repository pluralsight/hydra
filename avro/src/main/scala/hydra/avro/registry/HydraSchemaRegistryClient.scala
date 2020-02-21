package hydra.avro.registry

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient

/**
  * Created by alexsilva on 2/22/17.
  */
case class HydraSchemaRegistryClient(
    url: String,
    schemaRegistryClient: SchemaRegistryClient
)
