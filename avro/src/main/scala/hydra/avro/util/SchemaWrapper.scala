package hydra.avro.util

import org.apache.avro.Schema
import org.apache.avro.Schema.Field

import scala.collection.mutable
import scala.util.Try

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
case class SchemaWrapper(schema: Schema, primaryKeys: Seq[String]) {

  import scala.collection.JavaConverters._

  def validate(): Try[Unit] = {
    Try {
      val errors = primaryKeys.map(k => k -> Option(schema.getField(k))).filter(_._2.isEmpty).toMap
      if (!errors.isEmpty) {
        val err = s"The field(s) '${errors.keys.mkString(",")}' were specified as " +
          "primary keys, but they don't exist in the schema. " +
          s"Possible fields are: ${schema.getFields.asScala.map(_.name()).mkString(",")}. " +
          s"Please revise your request."
        throw new IllegalArgumentException(err)
      }
    }
  }

  def getFields: mutable.Buffer[Field] = schema.getFields.asScala

  def getName: String = schema.getName
}


object SchemaWrapper {
  def from(schema: Schema): SchemaWrapper = {
    SchemaWrapper(schema, schemaPKs(schema))
  }

  def from(schema: Schema, primaryKeys: Seq[String]): SchemaWrapper = {
    SchemaWrapper(schema, if (primaryKeys.isEmpty) schemaPKs(schema) else primaryKeys)
  }

  private def schemaPKs(schema: Schema): Seq[String] = {
    Option(schema.getProp("hydra.key"))
      .map(_.replaceAll("\\s", "").split(",")) match {
      case Some(ids) => ids
      case None => Seq.empty
    }
  }
}