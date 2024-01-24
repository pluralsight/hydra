package hydra.kafka.model

import akka.http.scaladsl.unmarshalling.Unmarshaller
import enumeratum.{Enum, EnumEntry}
import spray.json.DeserializationException

import scala.collection.immutable

sealed trait SkipValidation extends EnumEntry

object SkipValidation extends Enum[SkipValidation] {

  case object all extends SkipValidation
  case object emptyKeyFields extends SkipValidation
  case object keySchemaEvolution extends SkipValidation
  case object valueSchemaEvolution extends SkipValidation
  case object requiredDocField extends SkipValidation
  case object requiredCreatedAtField extends SkipValidation
  case object requiredUpdatedAtField extends SkipValidation
  case object sameFieldsTypeMismatchInKeyValueSchemas extends SkipValidation
  case object nullableFieldsInKeySchema extends SkipValidation
  case object missingDefaultInNullableFieldsOfValueSchema extends SkipValidation
  case object unsupportedLogicalTypeFieldsInKeySchema extends SkipValidation
  case object unsupportedLogicalTypeFieldsInValueSchema extends SkipValidation
  case object defaultLoopholeInRequiredField extends SkipValidation

  override val values: immutable.IndexedSeq[SkipValidation] = findValues

  implicit val skipValidationUnmarshaller: Unmarshaller[String, List[SkipValidation]] =
    Unmarshaller.strict[String, List[SkipValidation]] { commaSeparatedValidations =>
      val stringValues = values.map(_.entryName)
      val invalidValues = commaSeparatedValidations.split(",").filterNot(s => stringValues.contains(s) )
      val validValues = commaSeparatedValidations.split(",").flatMap(s => values.find(v => v.entryName == s.trim)).toList

      if (invalidValues.nonEmpty) {
        throw DeserializationException(
          s"Expected single or comma-separated values from enum[${values.mkString(", ")}] but received invalid value(s): ${invalidValues.mkString(",")}")
      }

      validValues
    }
}
