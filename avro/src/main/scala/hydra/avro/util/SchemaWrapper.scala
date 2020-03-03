package hydra.avro.util

import org.apache.avro.Schema
import org.apache.avro.Schema.{Field, Type}

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
      val mappedKeys = primaryKeys.map(k => k -> Option(schema.getField(k)))
      val errors = mappedKeys.filter(_._2.isEmpty).toMap
      if (!errors.isEmpty) {
        val err =
          s"The field(s) '${errors.keys.mkString(",")}' were specified as " +
            "primary keys, but they don't exist in the schema. " +
            s"Possible fields are: ${schema.getFields.asScala.map(_.name()).mkString(",")}. " +
            s"Please revise your request."
        throw new IllegalArgumentException(err)
      }

      val unionMap = primaryKeys
        .map(schema.getField)
        .map(f => f.name() -> isNullableUnion(f.schema()))
        .filter(_._2)
        .toMap

      if (!unionMap.isEmpty) {
        val err =
          s"The field(s) '${unionMap.keys.mkString(",")}' were specified as " +
            "primary keys, but they are nullable.  This is currently not supported."
        throw new IllegalArgumentException(err)
      }

    }
  }

  def getFields: mutable.Buffer[Field] = schema.getFields.asScala

  private def isNullableUnion(schema: Schema): Boolean =
    schema.getType == Type.UNION && schema.getTypes.size == 2 && schema
      .getTypes()
      .contains(Schema.create(Type.NULL))

  def getName: String = schema.getName
}

object SchemaWrapper {

  def from(schema: Schema): SchemaWrapper = {
    SchemaWrapper(schema, schemaPKs(schema))
  }

  /**
    *
    * @param schema The avro schema
    * @param primaryKeys A sequence where the values are field names to be used as primary keys.
    *                    This will override the `hydra.keys` schema property.
    *                    For streaming materialization jobs where no primary key is to be used,
    *                    an empty sequence should be provided as this method's argument.
    * @return a SchemaWrapper instance
    */
  def from(schema: Schema, primaryKeys: Seq[String]): SchemaWrapper = {
    SchemaWrapper(schema, primaryKeys)
  }

  private def schemaPKs(schema: Schema): Seq[String] = {
    Option(schema.getProp("hydra.key"))
      .map(_.replaceAll("\\s", "").split(",")) match {
      case Some(ids) => ids
      case None      => Seq.empty
    }
  }
}
