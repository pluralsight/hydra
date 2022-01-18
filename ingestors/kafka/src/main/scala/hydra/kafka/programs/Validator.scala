package hydra.kafka.programs

import cats.MonadThrow
import cats.syntax.all._
import cats.data.{NonEmptyChain, Validated, ValidatedNec}
import hydra.kafka.programs.ValidationError.ValidationCombinedErrors

trait Validator {
  import Validator._

  protected def resultOf[F[_]](chain: ValidationChain*)(implicit eff: MonadThrow[F]): F[Unit] =
    eff.fromEither(chain.toList.sequence.toEither.bimap(errors => merge(errors.toList), _ => ()))

  protected def resultOf[F[_]: MonadThrow](chainF: F[List[ValidationChain]]): F[Unit] =
    chainF.map(_.sequence.toEither.bimap(errors => merge(errors.toList), _ => ())).rethrow

  protected def validate(test: Boolean, error: => ValidationError): ValidationChain =
    if (test) valid else Validated.Invalid(NonEmptyChain.one(error))

  private def merge(errors: List[ValidationError]): ValidationError =
    if (errors.size == 1) errors.head else ValidationCombinedErrors(errors.map(_.message))
}

object Validator {
  type ValidationChain = ValidatedNec[ValidationError, Any]

  val valid: ValidationChain = Validated.Valid(())
}
