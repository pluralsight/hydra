package hydra.avro.resource

import cats.data._
import cats.implicits._

trait HydraSubjectValidator {
  import HydraSubjectValidator._
  private val kafkaValidCharacterRegex = """^[a-zA-Z0-9_\-.]*$""".r

  type ValidationResult[A] = ValidatedNec[SubjectValidation, A]

  private[resource] def validateSubjectCharacters(subject: String): ValidationResult[String] =
    if (kafkaValidCharacterRegex.pattern.matcher(subject).matches) subject.validNec
    else SubjectHasInvalidCharacters.invalidNec
}

object HydraSubjectValidator {
  sealed trait SubjectValidation {
    def errorMessage: String
  }

  case object SubjectHasInvalidCharacters extends SubjectValidation {
    val errorMessage = "Subject can only contain numbers, letters, hyphens, periods, and underscores."
  }
}