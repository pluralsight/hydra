package hydra.kafka.programs

import hydra.kafka.model.RequiredField
import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.Schema.Type

import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

object RequiredFieldStructures {

  def docFieldValidator(schema: Schema): Boolean =
    schema.getFields.asScala.toList.forall(d => Option(d.doc()).nonEmpty)

  def createdAtFieldValidator(schema: Schema): Boolean =
    schema.getFields.asScala.toList.find(_.name == RequiredField.CREATED_AT).exists(checkFields)

  def updatedAtFieldValidator(schema: Schema): Boolean =
    schema.getFields.asScala.toList.find(_.name == RequiredField.UPDATED_AT).exists(checkFields)

  private def checkFields(field: Schema.Field) =
    field.schema.getLogicalType == LogicalTypes.timestampMillis && field.schema.getType == Type.LONG

  def defaultFieldOfRequiredFieldValidator(schema: Schema, field: String, isCreatedAfterCutOffDate: Boolean): Boolean =
    if (isCreatedAfterCutOffDate) schema.getFields.asScala.toList.find(_.name == field).exists(!_.hasDefaultValue) else true
}
