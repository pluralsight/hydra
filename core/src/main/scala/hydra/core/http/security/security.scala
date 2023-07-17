package hydra.core.http.security

package object security {
  sealed trait StrValue extends Any {
    def value: String

    override def toString: String = value
  }

  final case class AccessKeyId(value: String) extends AnyVal with StrValue

  final case class SecretAccessKey(value: String) extends AnyVal with StrValue

  final case class SessionToken(value: String) extends AnyVal with StrValue

  final case class PolicyName(value: String) extends AnyVal with StrValue

  final case class RoleName(value: String) extends AnyVal with StrValue

  final case class VersionId(value: String) extends AnyVal with StrValue
}
