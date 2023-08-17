package hydra.kafka.programs

import cats.effect.Sync
import cats.syntax.all._
import hydra.common.validation.Validator.ValidationChain
import hydra.common.validation.{ValidationError, Validator}
import hydra.kafka.algebras.MetadataAlgebra
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model.{MetadataValidationType, TopicMetadataV2Request, ValidationType}

import scala.language.higherKinds

class MetadataV2Validator[F[_] : Sync](metadataAlgebra: MetadataAlgebra[F]) extends Validator {

  def validate(request: TopicMetadataV2Request, subject: Subject): F[Unit] =
    for {
      metadata    <- metadataAlgebra.getMetadataFor(subject)
      validations <- ValidationType.metadataValidations(metadata).getOrElse(List.empty).pure
      _           <- resultOf(validate(validations, request, subject))
    } yield()

  private def validate(validations: List[MetadataValidationType], request: TopicMetadataV2Request, subject: Subject): F[List[ValidationChain]] =
    (validations flatMap {
      case MetadataValidationType.replacementTopics => validateReplacementTopics(request.deprecated, request.replacementTopics, subject.value)
      case MetadataValidationType.previousTopics => validatePreviousTopics(request.previousTopics)
    }).pure

  private def validateReplacementTopics(deprecated: Boolean, replacementTopics: Option[List[String]], topic: String): List[ValidationChain] = {
    validateDeprecatedTopicHasReplacementTopic(deprecated, replacementTopics, topic) +:
      validateTopicsFormat(replacementTopics)
  }

  private def validatePreviousTopics(previousTopics: Option[List[String]]): List[ValidationChain] = {
    validateTopicsFormat(previousTopics)
  }

  private def validateDeprecatedTopicHasReplacementTopic(deprecated: Boolean, replacementTopics: Option[List[String]], topic: String): ValidationChain = {
    val hasReplacementTopicsIfDeprecated = if (deprecated) replacementTopics.exists(_.nonEmpty) else true
    validate(hasReplacementTopicsIfDeprecated, ReplacementTopicsMissingError(topic))
  }

  private def validateTopicsFormat(maybeTopics: Option[List[String]]): List[ValidationChain] = {
    val validations = for {
      topics <- maybeTopics
    } yield {
      topics map { topic =>
        val isValidTopicFormat = Subject.createValidated(topic).nonEmpty
        validate(isValidTopicFormat, InvalidTopicFormatError(topic))
      }
    }
    validations.getOrElse(List.empty)
  }
}

sealed trait MetadataValidationError extends ValidationError

case class ReplacementTopicsMissingError(topic: String) extends MetadataValidationError {
  override def message: String = s"Field 'replacementTopics' is required when the topic '$topic' is being deprecated!"
}

case class InvalidTopicFormatError(topic: String) extends MetadataValidationError {
  override def message: String =  s"$topic : " + Subject.invalidFormat
}


object MetadataV2Validator {
  def make[F[_] : Sync](metadataAlgebra: MetadataAlgebra[F]): MetadataV2Validator[F] =
    new MetadataV2Validator[F](metadataAlgebra)
}