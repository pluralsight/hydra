package hydra.core.http.security

import cats.MonadThrow
import cats.syntax.all._
import hydra.common.validation.Validator
import hydra.core.http.security.entity.SecurityValidationError.{EmptyAccessKeyId, EmptySecretAccessKey, EmptySessionToken}
import security._

class AwsSecurityValidator[F[_]: MonadThrow] private extends Validator {
  def validateTokens(accessKeyId: AccessKeyId,
                     secretAccessKey: SecretAccessKey,
                     sessionTokenOpt: SessionToken): F[Unit] =
    resultOf(
      validate(accessKeyId.value.trim.nonEmpty, EmptyAccessKeyId),
      validate(secretAccessKey.value.trim.nonEmpty, EmptySecretAccessKey),
      validate(sessionTokenOpt.value.trim.nonEmpty, EmptySessionToken)
    )
}

object AwsSecurityValidator {
  def make[F[_]: MonadThrow]: F[AwsSecurityValidator[F]] = new AwsSecurityValidator[F].pure
}
