package hydra.kafka.services

import cats.data._
import cats.implicits._

/*
    V2 Subject Validator - mimics limited functionality of the TopicMetadataValidator
 */
trait HydraSubjectValidator {
  import HydraSubjectValidator._
  private val kafkaValidCharacterRegex = """^[a-zA-Z0-9_\-.]*$""".r

  type ValidationResult[A] = ValidatedNec[SubjectValidation, A]

  private[services] def validateSubjectCharacters(subject: String): ValidationResult[String] =
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