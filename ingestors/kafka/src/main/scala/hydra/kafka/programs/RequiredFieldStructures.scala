package hydra.kafka.programs

import hydra.kafka.model.RequiredField
import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.Schema.Type

import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

object RequiredFieldStructures {

  def docFieldValidator(schema: Schema): Boolean =
    schema.getFields.asScala.toList.forall(d => Option(d.doc()).nonEmpty)

  def createdAtFieldValidator(schema: Schema): Boolean =
    schema.getFields.asScala.toList.find(_.name == RequiredField.CREATED_AT).exists(validateTimestampType)

  def updatedAtFieldValidator(schema: Schema): Boolean =
    schema.getFields.asScala.toList.find(_.name == RequiredField.UPDATED_AT).exists(validateTimestampType)

  private def validateTimestampType(field: Schema.Field) =
    field.schema.getLogicalType == LogicalTypes.timestampMillis && field.schema.getType == Type.LONG

  def defaultFieldOfRequiredFieldValidator(schema: Schema, field: String): Boolean = {
    val requiredField = schema.getFields.asScala.toList.find(_.name == field)
    if (requiredField.nonEmpty) requiredField.exists(!_.hasDefaultValue) else true
  }
}
