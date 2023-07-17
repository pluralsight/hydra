package hydra.core.http.security

import cats.effect.Sync
import cats.syntax.all._
import com.amazonaws.auth.policy.Policy
import com.amazonaws.auth.policy.internal.{JsonPolicyReader, JsonPolicyWriter}
import hydra.core.http.security.entity.AwsIamError
import org.typelevel.log4cats.Logger
import software.amazon.awssdk.arns.Arn
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.iam.IamClient
import security._
import software.amazon.awssdk.auth.credentials.{DefaultCredentialsProvider, StaticCredentialsProvider}
import software.amazon.awssdk.services.iam.model.{AttachedPolicy, DeleteRolePolicyRequest, GetPolicyRequest, GetPolicyVersionRequest, GetRolePolicyRequest, ListAttachedRolePoliciesRequest, ListRolePoliciesRequest, NoSuchEntityException, PolicyEvaluationDecisionType, PutRolePolicyRequest, SimulateCustomPolicyRequest}
import software.amazon.awssdk.services.iam.model.{Policy => ManagedPolicy}

import java.net.URLDecoder
import scala.collection.JavaConverters._

trait AwsIamClient[F[_]] {
  def listRolePoliciesNames(roleName: RoleName): F[Vector[String]]

  def getRolePolicy(roleName: RoleName, policyName: PolicyName): F[Option[Policy]]

  def listAttachedRolePolicies(roleName: RoleName): F[Vector[AttachedPolicy]]

  def getPolicy(policyArn: Arn): F[Option[ManagedPolicy]]

  def getPolicyVersion(policyArn: Arn, versionId: VersionId): F[Option[Policy]]

  def putRolePolicy(roleName: RoleName, policyName: PolicyName, policy: Policy): F[Unit]

  def deleteRolePolicy(roleName: RoleName, policyName: PolicyName): F[Unit]

  def allowedToDoActionsWithTopic(actions: Seq[AwsIamPolicyAction.KafkaAction], kafkaTopicArn: Vector[Arn], policies: Vector[Policy]): F[Boolean]

}

object AwsIamClient {
  def make[F[_]: Logger](implicit eff: Sync[F]): F[AwsIamClient[F]] = {
    eff.delay {
      new AwsIamClient[F] {
        override def listRolePoliciesNames(roleName: RoleName): F[Vector[String]] =
          eff.delay(iamClient.listRolePolicies(ListRolePoliciesRequest.builder().roleName(roleName.value).build()))
            .map(_.policyNames().asScala.toVector)
            .recover { case _: NoSuchEntityException => Vector.empty[String] }
            .handleErrorWith(e => Logger[F].error(e)(s"AwsIamClient: Error on getting list of role [$roleName] policies") *> e.raiseError)
            .adaptError { case _ => AwsIamError.CannotGetPoliciesNamesForRole(roleName) }

        override def getRolePolicy(roleName: RoleName, policyName: PolicyName): F[Option[Policy]] =
          eff.delay(iamClient.getRolePolicy(GetRolePolicyRequest.builder().roleName(roleName.value).policyName(policyName.value).build()))
            .map(response => fromJsonStringToPolicy(response.policyDocument()).some)
            .recover { case _: NoSuchEntityException => none }
            .handleErrorWith(e => Logger[F].error(e)(s"AwsIamClient: Error on getting policy [$policyName] for role [$roleName]") *> e.raiseError)
            .adaptError { case _ => AwsIamError.CannotGetRolePolicy(roleName, policyName) }

        override def listAttachedRolePolicies(roleName: RoleName): F[Vector[AttachedPolicy]] =
          eff.delay(iamClient.listAttachedRolePolicies(ListAttachedRolePoliciesRequest.builder().roleName(roleName.value).build()))
            .map(response => response.attachedPolicies().asScala.toVector)
            .recover { case _: NoSuchEntityException => Vector.empty[AttachedPolicy] }
            .handleErrorWith(e => Logger[F].error(e)(s"AwsIamClient: Error on getting inline policies for role [$roleName]") *> e.raiseError)
            .adaptError { case _ => AwsIamError.CannotGetAttachedRolePolicies(roleName) }

        override def getPolicy(policyArn: Arn): F[Option[ManagedPolicy]] =
          eff.delay(iamClient.getPolicy(GetPolicyRequest.builder().policyArn(policyArn.toString).build()))
            .map(_.policy().some)
            .recover { case _: NoSuchEntityException => none }
            .handleErrorWith(e => Logger[F].error(e)(s"AwsIamClient: Error on getting policy data by arn [$policyArn]") *> e.raiseError)
            .adaptError { case _ => AwsIamError.CannotGetPolicyByArn(policyArn) }

        override def getPolicyVersion(policyArn: Arn, versionId: VersionId): F[Option[Policy]] =
          eff.delay(iamClient.getPolicyVersion(GetPolicyVersionRequest.builder().policyArn(policyArn.toString).versionId(versionId.value).build()))
            .map(response => fromJsonStringToPolicy(response.policyVersion().document()).some)
            .recover { case _: NoSuchEntityException => none }
            .handleErrorWith(e => Logger[F].error(e)(s"AwsIamClient: Error on getting policy version data by arn [$policyArn] and version id [${versionId}]") *> e.raiseError)
            .adaptError { case _ => AwsIamError.CannotGetPolicyVersion(policyArn, versionId) }

        override def putRolePolicy(roleName: RoleName, policyName: PolicyName, policy: Policy): F[Unit] =
          eff.delay(iamClient.putRolePolicy(PutRolePolicyRequest.builder().roleName(roleName.value).policyName(policyName.value).policyDocument(fromPolicyToJsonString(policy)).build()))
            .map(_ => ())
            .recover { case _: NoSuchEntityException => () }
            .handleErrorWith(e => Logger[F].error(e)(s"AwsIamClient: Error on putting policy [$policyName] for role [$roleName]") *> e.raiseError)
            .adaptError { case _ => AwsIamError.CannotPutRolePolicy(roleName, policyName) }

        override def deleteRolePolicy(roleName: RoleName, policyName: PolicyName): F[Unit] =
          eff.delay(iamClient.deleteRolePolicy(DeleteRolePolicyRequest.builder().roleName(roleName.value).policyName(policyName.value).build()))
            .map(_ => ())
            .recover { case _: NoSuchEntityException => () }
            .handleErrorWith(e => Logger[F].error(e)(s"AwsIamClient: Error on deleting policy [$policyName] for role [$roleName]") *> e.raiseError)
            .adaptError { case _ => AwsIamError.CannotDeleteRolePolicy(roleName, policyName) }

        override def allowedToDoActionsWithTopic(actions: Seq[AwsIamPolicyAction.KafkaAction], kafkaTopicArns: Vector[Arn], policies: Vector[Policy]): F[Boolean] = {
          val actionsNames = actions.map(_.getActionName)
          val policiesJson = policies.map(fromPolicyToJsonString)

          val request = SimulateCustomPolicyRequest
            .builder()
            .actionNames(actionsNames.asJavaCollection)
            .policyInputList(policiesJson.asJavaCollection)
            .resourceArns(if (kafkaTopicArns.isEmpty) Vector("*").asJavaCollection else kafkaTopicArns.map(_.toString).asJavaCollection)
            .build()

          eff.delay(iamClient.simulateCustomPolicy(request))
            .map(_.evaluationResults().asScala.toVector.exists(_.evalDecision() == PolicyEvaluationDecisionType.ALLOWED))
            .adaptError { case e => AwsIamError.CannotCheckPolicies(e) }
            .handleErrorWith(e => Logger[F].error(e)(s"AwsIamClient: Error on checking policies with actions [$actions] for topicArns [$kafkaTopicArns].") *> e.raiseError)
        }

        private def fromJsonStringToPolicy(json: String) =
          (new JsonPolicyReader).createPolicyFromJsonString(URLDecoder.decode(json, "UTF-8"))

        private def fromPolicyToJsonString(policy: Policy) =
          (new JsonPolicyWriter).writePolicyToString(policy)

      }
    }
  }

  private val IamGlobalRegion = Region.AWS_GLOBAL

  private val iamClient = IamClient.builder().region(IamGlobalRegion)
    .credentialsProvider(DefaultCredentialsProvider.create()).build()
}
