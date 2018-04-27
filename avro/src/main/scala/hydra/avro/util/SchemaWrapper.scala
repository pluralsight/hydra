package hydra.avro.util

import org.apache.avro.Schema
import org.apache.avro.Schema.Field

import scala.collection.mutable

/**
  * A wrapper class that holds both a schema and an optional list of primary keys.
  *
  * Primary keys are defined one of two ways:
  *
  * 1. Adding a property named "hydra.key" to the avro schema,which can contain a single field name
  * or a comma delimmited list of field names (for composite primary keys.)
  *
  * 2. By providing a Seq of field names, which _will_ take precedence over any schema property.
  *
  * @param schema      The avro schema
  * @param primaryKeys The list of primary keys; Empty if no primary keys.
  */
case class SchemaWrapper(schema: Schema, primaryKeys: Seq[Field]) {

  import scala.collection.JavaConverters._

  def getFields: mutable.Buffer[Field] = schema.getFields.asScala

  def getName: String = schema.getName
}


object SchemaWrapper {
  def from(schema: Schema): SchemaWrapper = {
    SchemaWrapper(schema, schemaPKs(schema))
  }

  def from(schema: Schema, primaryKeys: Seq[Field]): SchemaWrapper = {
    SchemaWrapper(schema, if (primaryKeys.isEmpty) schemaPKs(schema) else primaryKeys)
  }

  private def schemaPKs(schema: Schema): Seq[Field] = {
    Option(schema.getProp("hydra.key"))
      .map(_.replaceAll("\\s", "").split(",")) match {
      case Some(ids) => ids.map(AvroUtils.getField(_, schema))
      case None => Seq.empty
    }
  }
}