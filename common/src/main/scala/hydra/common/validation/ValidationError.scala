package hydra.common.validation

import scala.util.control.NoStackTrace

trait ValidationError extends NoStackTrace {
  def message: String

  override def getMessage: String = message
}

object ValidationError {
  case class ValidationCombinedErrors(errors: List[String]) extends ValidationError {
    override val message: String = errors.mkString("\n")
  }
}