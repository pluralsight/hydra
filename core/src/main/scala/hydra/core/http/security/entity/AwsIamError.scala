package hydra.core.http.security.entity

import hydra.core.http.security.security._
import software.amazon.awssdk.arns.Arn

import scala.util.control.NoStackTrace

trait AwsIamError extends NoStackTrace {
  def message: String

  override def getMessage: String = message
}

object AwsIamError {

  final case class CannotRetrieveRoleName(callerIdentityArn: Arn) extends AwsIamError {
    override def message: String = s"Cannot get role name from IAM from caller identity ARN ${callerIdentityArn.toString}"
  }

  final case object MissingAccessKeyIdField extends AwsIamError {
    override def message: String = "Access Key Id field is mandatory."
  }

  final case object MissingSecretAccessKey extends AwsIamError {
    override def message: String = "Secret Access Key field is mandatory."
  }

  final case object MissingSessionToken extends AwsIamError {
    override def message: String = "Session token is mandatory."
  }

  final case class RoleDoesntHavePolicies(roleName: RoleName) extends AwsIamError {
    override def message: String = s"Role [$roleName] doesn't have policies."
  }

  final case class CannotGetPoliciesNamesForRole(roleName: RoleName) extends AwsIamError {
    override def message: String = s"Cannot get listRolePolicies names for role [$roleName]."
  }

  final case class CannotGetRolePolicy(roleName: RoleName, policyName: PolicyName) extends AwsIamError {
    override def message: String = s"Cannot get getRolePolicy for role [$roleName] and policy [$policyName]."
  }

  final case class CannotGetAttachedRolePolicies(roleName: RoleName) extends AwsIamError {
    override def message: String = s"Cannot get listAttachedRolePolicies for role [$roleName]"
  }

  final case class CannotGetPolicyByArn(policyArn: Arn) extends AwsIamError {
    override def message: String = s"Cannot get getPolicy by policy arn [$policyArn]."
  }

  final case class CannotGetPolicyVersion(policyArn: Arn, versionId: VersionId) extends AwsIamError {
    override def message: String = s"Cannot get getPolicyVersion by policy arn [$policyArn] and version Id [$versionId]."
  }

  final case class CannotPutRolePolicy(roleName: RoleName, policyName: PolicyName) extends AwsIamError {
    override def message: String = s"Cannot put a policy [$policyName] for role [$roleName]."
  }

  final case class CannotDeleteRolePolicy(roleName: RoleName, policyName: PolicyName) extends AwsIamError {
    override def message: String = s"Cannot delete a policy [$policyName] for role [$roleName]."
  }

  final case class CannotCheckPolicies(ex: Throwable) extends AwsIamError {
    override def message: String = s"Cannot check policies: ${ex.getMessage}."
  }

  final case object CannotGetCallerIdentity extends AwsIamError {
    override def message: String = s"Cannot get caller identity by tokens."
  }

  final case class AuthorizationServiceError(error: Throwable) extends AwsIamError {
    override def message: String = s"Authorization error: ${error.getMessage}"
  }
}
