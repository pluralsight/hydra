package hydra.core.avro.registry

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import scala.collection.JavaConverters._

/**
  * Created by alexsilva on 2/21/17.
  */
trait SchemaRegistryComponent {
  val registry: SchemaRegistryClient

}
