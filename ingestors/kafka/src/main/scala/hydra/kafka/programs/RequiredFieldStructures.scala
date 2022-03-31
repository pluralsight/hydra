package hydra.kafka.programs

import hydra.kafka.model.RequiredField
import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.Schema.Type

import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

object RequiredFieldStructures {

  def docFieldValidator(schema: Schema): Boolean =
    schema.getFields.asScala.toList.find(_.name == RequiredField.DOC).exists(_.schema.getType == Type.STRING)

  def createdAtFieldValidator(schema: Schema): Boolean =
    schema.getFields.asScala.toList.find(_.name == RequiredField.CREATED_AT).exists{
      field =>
        field.schema.getLogicalType == LogicalTypes.timestampMillis && field.schema.getType == Type.LONG
    }

  def updatedAtFieldValidator(schema: Schema): Boolean =
    schema.getFields.asScala.toList.find(_.name == RequiredField.UPDATED_AT).exists{
      field =>
        field.schema.getLogicalType == LogicalTypes.timestampMillis && field.schema.getType == Type.LONG
    }
}
