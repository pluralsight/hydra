package hydra.core.http.security

import cats.effect.Sync
import cats.syntax.all._
import com.amazonaws.auth.policy.Policy
import hydra.core.http.security.entity._
import org.typelevel.log4cats.Logger
import software.amazon.awssdk.arns.Arn
import security._

trait AwsIamService[F[_]] {
  def getRoleName(accessKeyId: AccessKeyId, secretAccessKey: SecretAccessKey, sessionTokenOpt: SessionToken): F[RoleName]

  def getRolePolicies(roleName: RoleName): F[Vector[Policy]]

  def addRolePolicy(roleName: RoleName, policyName: PolicyName, policy: Policy): F[Unit]

  def deleteRolePolicy(roleName: RoleName, policyName: PolicyName): F[Unit]

  def allowedToDoActionsWithTopic(actions: Seq[AwsIamPolicyAction.KafkaAction], kafkaTopicArn: Vector[Arn], policies: Vector[Policy]): F[Boolean]
}

object AwsIamService {
  def make[F[_]: Logger](awsIamClient: AwsIamClient[F],
                 awsStsClient: AwsStsClient[F])(implicit eff: Sync[F]): F[AwsIamService[F]] = {
    eff.delay {
      new AwsIamService[F] {
        override def getRoleName(accessKeyId: AccessKeyId,
                                 secretAccessKey: SecretAccessKey,
                                 sessionToken: SessionToken): F[RoleName] = {
          def getRoleNameFormArn(arn: Arn): Option[String] = {
            val resourceTypeOptJava = arn.resource().resourceType()
            val resourceTypeOpt = if (resourceTypeOptJava.isPresent) Some(resourceTypeOptJava.get) else None

            resourceTypeOpt match {
              case Some(resourceType) if resourceType == "assumed-role" => arn.resource().resource().some
              case _ => none
            }
          }

          for {
            callerIdentityArn <- awsStsClient.getCallerIdentityArn(accessKeyId, secretAccessKey, sessionToken)
            roleNameOpt       = getRoleNameFormArn(callerIdentityArn)
            roleName          <- eff.fromOption(roleNameOpt, AwsIamError.CannotRetrieveRoleName(callerIdentityArn))
          } yield RoleName(roleName)
        }

        override def getRolePolicies(roleName: RoleName): F[Vector[Policy]] = {
          def getDefaultManagedPolicy(policyArn: Arn): F[Option[Policy]] =
            for {
              managedPolicyOpt <- awsIamClient.getPolicy(policyArn)
              defaultPolicyOpt <- managedPolicyOpt.flatTraverse(managedPolicy => awsIamClient.getPolicyVersion(policyArn, VersionId(managedPolicy.defaultVersionId())))
            } yield defaultPolicyOpt

          for {
            inlinePoliciesNames    <- awsIamClient.listRolePoliciesNames(roleName)
            inlinePolicyDocuments  <- inlinePoliciesNames.traverse(policyName => awsIamClient.getRolePolicy(roleName, PolicyName(policyName)))
            managedPolicies        <- awsIamClient.listAttachedRolePolicies(roleName)
            managedPolicyDocuments <- managedPolicies.traverse(managedPolicy => getDefaultManagedPolicy(Arn.fromString(managedPolicy.policyArn())))
            allPolicies            = (inlinePolicyDocuments ++ managedPolicyDocuments).flatten
            _                      <- Logger[F].info(s"AwsIamService: Retrieved [${allPolicies.size}] policies for a role [$roleName]")
          } yield allPolicies
        }

        override def addRolePolicy(roleName: RoleName, policyName: PolicyName, policy: Policy): F[Unit] =
          awsIamClient.putRolePolicy(roleName, policyName, policy) *>
            Logger[F].info(s"AwsIamService: Added new policy [$policyName] for a role [$roleName]")

        override def deleteRolePolicy(roleName: RoleName, policyName: PolicyName): F[Unit] =
          awsIamClient.deleteRolePolicy(roleName, policyName) *>
            Logger[F].info(s"AwsIamService: Deleted a policy [$policyName] for a role [$roleName]")

        override def allowedToDoActionsWithTopic(actions: Seq[AwsIamPolicyAction.KafkaAction], kafkaTopicArns: Vector[Arn], policies: Vector[Policy]): F[Boolean] =
          awsIamClient.allowedToDoActionsWithTopic(actions, kafkaTopicArns, policies)

      }
    }
  }
}
