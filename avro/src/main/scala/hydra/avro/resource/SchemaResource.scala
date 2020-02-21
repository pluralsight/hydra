package hydra.avro.resource

import org.apache.avro.Schema

case class SchemaResource(id: Int, version: Int, schema: Schema)
