package hydra.core.http.security.entity

import hydra.common.validation.ValidationError

sealed trait SecurityValidationError extends ValidationError

object SecurityValidationError {

  final case object EmptyAccessKeyId extends SecurityValidationError {
    override def message: String = "Access Key Id is empty."
  }

  final case object EmptySecretAccessKey extends SecurityValidationError {
    override def message: String = "Secret Access Key is empty."
  }

  final case object EmptySessionToken extends SecurityValidationError {
    override def message: String = "Session Token is empty."
  }

}
