package hydra.avro.resource

import org.apache.avro.Schema
import cats.data._
import cats.data.Validated._
import cats.implicits._


case class HydraSubject(keySchema: SchemaResource, valueSchema: SchemaResource)

case class SubjectRegistration(keySchema: Schema, valueSchema: Schema)

sealed trait SubjectValidation {
  def errorMessage: String
}

case object SchemaHasInvalidCharacters extends SubjectValidation {
  val errorMessage = "Schema names can only contain numbers, letters and underscores."
}

case object SchemaNamesDoNotMatch extends SubjectValidation {
  val errorMessage = "Key schema name must match value schema name (after -key and -value suffixes are dropped.)"
}

case object InvalidSchemaSuffix extends SubjectValidation {
  val errorMessage = "Key schemas must end in `-key` and value schemas must end in `-value`."
}

sealed trait HydraSubjectValidator {

  val validSchemaNameRegex = "^[a-zA-Z0-9]*$".r

  type ValidationResult[A] = ValidatedNec[SubjectValidation, A]

  def validateSchemaName(schemaName: String): ValidationResult[String] =
    if (validSchemaNameRegex.pattern.matcher(schemaName).matches) schemaName.validNec
    else SchemaHasInvalidCharacters.invalidNec

  def validateNamesMatch(keySchemaName: String, valueSchemaName: String): ValidationResult[Boolean] =
    if (keySchemaName.dropRight(4) == valueSchemaName.dropRight(6)) true.validNec else SchemaNamesDoNotMatch.invalidNec


  def validateSchema(schemaStr: String, suffix: String): ValidationResult[Schema] = {
    val schema = new Schema.Parser().parse(schemaStr)
    if (schema.getName.endsWith(suffix)) schema.validNec else InvalidSchemaSuffix.invalidNec
  }

  def validateSubject(keySchema: String, valueSchema: String): ValidationResult[SubjectRegistration] = {
    (validateSchemaName(keySchema),
      validateSchemaName(valueSchema),
      validateNamesMatch(keySchema, valueSchema),
      validateSchema(keySchema, "-key"),
      validateSchema(valueSchema, "-value")).mapN { case (_, _, _, ks, vs) => SubjectRegistration(ks, vs)
    }
  }

  object HydraSubjectValidator extends HydraSubjectValidator