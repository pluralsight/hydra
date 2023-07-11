package hydra.kafka.programs

import hydra.common.validation.ValidationError
import org.apache.avro.Schema

sealed trait TopicSchemaError extends ValidationError

object TopicSchemaError {
  case object InvalidSchemaTypeError extends TopicSchemaError {
    override val message: String = "Your key and value schemas must each be of type record. If you are adding metadata for a topic you created externally, you will need to register new key and value schemas"
  }

  case object KeyIsEmptyError extends TopicSchemaError {
    override val message: String = "Must include Fields in Key"
  }

  case class IllegalLogicalTypeChangeError(originalType: String, proposedType: String, fieldName: String) extends TopicSchemaError {
    override val message: String = s"Changing logical types is not allowed. Field named '$fieldName's logical type cannot be changed from " +
      s"logicalType of '$originalType' to logicalType of '$proposedType'"
  }

  case class KeyHasNullableFieldError(fieldName: String, keyFieldSchema: Schema) extends TopicSchemaError {
    override val message: String = s"Fields within the key object cannot be nullable: field name = $fieldName, key schema = $keyFieldSchema."
  }

  case class NullableFieldWithoutDefaultValueError(fieldName: String, valueFieldSchema: Schema) extends TopicSchemaError {
    override val message: String = s"Nullable field should have default value: field name = $fieldName, key schema = $valueFieldSchema."
  }

  case class IncompatibleKeyAndValueFieldNamesError(fieldName: String, keyFieldSchema: Schema, valueFieldSchema: Schema) extends TopicSchemaError {
    override val message: String = s"Fields with same names in key and value schemas must have same type: field name = $fieldName, key schema = $keyFieldSchema, value schema = $valueFieldSchema."
  }

  case class UnsupportedLogicalType(unsupportedField: Schema.Field, unsupportedLogicalType: String) extends TopicSchemaError {
    override val message: String =  s"Field named '${unsupportedField.name()}' has unsupported logical type '$unsupportedLogicalType'"
  }

  case class RequiredSchemaKeyFieldMissingError(fieldName: String, keyFieldSchema: Schema, streamType: String) extends TopicSchemaError {
    override val message: String = s"Required field missing for the key schema fields: field name = $fieldName, " +
      s"key schema = $keyFieldSchema, stream type = $streamType."
  }

  case class RequiredSchemaValueFieldMissingError(fieldName: String, valueFieldSchema: Schema, streamType: String) extends TopicSchemaError {
    override val message: String = s"Required field missing for the value schema fields: field name = $fieldName, " +
      s"value schema = $valueFieldSchema, stream type = $streamType."
  }

  def getFieldMissingError(isKey: Boolean, fieldName: String, schema: Schema, streamType: String): TopicSchemaError = {
    if (isKey) RequiredSchemaKeyFieldMissingError(fieldName, schema, streamType) else RequiredSchemaValueFieldMissingError(fieldName, schema, streamType)
  }
}
