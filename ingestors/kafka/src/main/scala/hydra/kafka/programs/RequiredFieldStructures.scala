package hydra.kafka.programs

import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.Schema.Type

import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

object RequiredFieldStructures {

  def docFieldValidator(schema: Schema): Boolean =
    schema.getFields.asScala.toList.find(_.name == "doc").exists(_.schema.getType == Type.STRING)

  def createdAtFieldValidator(schema: Schema): Boolean =
    schema.getFields.asScala.toList.find(_.name == "createdAt").exists{
      field =>
        field.schema.getLogicalType == LogicalTypes.timestampMillis && field.schema.getType == Type.LONG
    }

  def updatedAtFieldValidator(schema: Schema): Boolean =
    schema.getFields.asScala.toList.find(_.name == "updatedAt").exists{
      field =>
        field.schema.getLogicalType == LogicalTypes.timestampMillis && field.schema.getType == Type.LONG
    }
}
