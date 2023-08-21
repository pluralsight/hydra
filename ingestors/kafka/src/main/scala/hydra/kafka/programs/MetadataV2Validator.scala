package hydra.kafka.programs

import cats.effect.Sync
import cats.syntax.all._
import hydra.common.validation.Validator.ValidationChain
import hydra.common.validation.{ValidationError, Validator}
import hydra.kafka.algebras.MetadataAlgebra
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model.{AdditionalValidation, MetadataAdditionalValidation, TopicMetadataV2Request}

import scala.language.higherKinds

class MetadataV2Validator[F[_] : Sync](metadataAlgebra: MetadataAlgebra[F]) extends Validator {

  def validate(request: TopicMetadataV2Request, subject: Subject): F[Unit] =
    for {
      metadata              <- metadataAlgebra.getMetadataFor(subject)
      additionalValidations <- AdditionalValidation.metadataValidations(metadata).getOrElse(List.empty).pure
      _                     <- resultOf(validate(request))
      _                     <- resultOf(validateAdditional(additionalValidations, request, subject))
    } yield()

  private def validate(request: TopicMetadataV2Request): F[List[ValidationChain]] = {
    val validationResults = validateTopicsFormat(request.replacementTopics) ++
      validateTopicsFormat(request.previousTopics)
    validationResults.pure
  }

  private def validateAdditional(additionalValidations: List[MetadataAdditionalValidation],
                                 request: TopicMetadataV2Request,
                                 subject: Subject): F[List[ValidationChain]] =
    (additionalValidations flatMap {
      case MetadataAdditionalValidation.replacementTopics =>
        List(validateDeprecatedTopicHasReplacementTopic(request.deprecated, request.replacementTopics, subject.value))
    }).pure

  private def validateDeprecatedTopicHasReplacementTopic(deprecated: Boolean, replacementTopics: Option[List[String]], topic: String): ValidationChain = {
    val hasReplacementTopicsIfDeprecated = if (deprecated) replacementTopics.exists(_.nonEmpty) else true
    validate(hasReplacementTopicsIfDeprecated, ReplacementTopicsMissingError(topic))
  }

  private def validateTopicsFormat(maybeTopics: Option[List[String]]): List[ValidationChain] = {
    val validationResults = for {
      topics <- maybeTopics
    } yield {
      topics map { topic =>
        val isValidTopicFormat = Subject.createValidated(topic).nonEmpty
        validate(isValidTopicFormat, InvalidTopicFormatError(topic))
      }
    }
    validationResults.getOrElse(List.empty)
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