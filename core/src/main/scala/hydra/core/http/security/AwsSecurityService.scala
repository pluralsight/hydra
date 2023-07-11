package hydra.core.http.security

import cats.effect.Sync
import cats.syntax.all._
import com.amazonaws.auth.policy.{Action, Policy, Resource, Statement}
import hydra.core.http.security.AwsIamPolicyAction.KafkaAction
import hydra.core.http.security.entity.AwsIamError.MskClusterArnNotDefined
import hydra.core.http.security.entity.{AwsConfig, AwsIamError}
import hydra.core.http.security.security.{RoleName, SessionToken}
import org.typelevel.log4cats.Logger
import software.amazon.awssdk.arns.Arn
import security._

import scala.collection.JavaConverters._

trait AwsSecurityService[F[_]] {
  def getAssumedRoleName(accessKeyIdOpt: Option[AccessKeyId],
                         secretAccessKeyOpt: Option[SecretAccessKey],
                         sessionTokenOpt: Option[SessionToken]): F[RoleName]

  def isAssumedRoleAllowsToDoActions(actions: Seq[KafkaAction],
                                     topicNames: Vector[String],
                                     roleName: RoleName): F[Boolean]

  def addAllTopicPermissionsPolicy(topicName: String, roleName: RoleName): F[Unit]

  def deleteAllTopicPermissionsPolicy(topicName: String, roleName: RoleName): F[Unit]

  def deleteAllTopicPermissionsPolicy(topicNames: Vector[String], roleName: RoleName): F[Unit]
}

object AwsSecurityService {
  def make[F[_]: Logger](awsIamService: AwsIamService[F], awsConfig: AwsConfig)(implicit eff: Sync[F]): F[AwsSecurityService[F]] = {
    AwsSecurityValidator.make.map { validator =>
      new AwsSecurityService[F] {

        override def getAssumedRoleName(accessKeyIdOpt: Option[AccessKeyId],
                                        secretAccessKeyOpt: Option[SecretAccessKey],
                                        sessionTokenOpt: Option[SessionToken]): F[RoleName] = {
          for {
            accessKeyId     <- eff.fromOption(accessKeyIdOpt, AwsIamError.MissingAccessKeyIdField)
            secretAccessKey <- eff.fromOption(secretAccessKeyOpt, AwsIamError.MissingSecretAccessKey)
            sessionToken    <- eff.fromOption(sessionTokenOpt, AwsIamError.MissingSessionToken)
            _               <- validator.validateTokens(accessKeyId, secretAccessKey, sessionToken)
            roleName        <- awsIamService.getRoleName(accessKeyId, secretAccessKey, sessionToken)
            _               <- Logger[F].info(s"AwsSecurityService: Retrieved role name [$roleName] by tokens")
          } yield roleName
        }

        override def isAssumedRoleAllowsToDoActions(actions: Seq[KafkaAction],
                                                    topicNames: Vector[String],
                                                    roleName: RoleName): F[Boolean] =
          for {
            _               <- Logger[F].info(s"AwsSecurityService: Going to check if role's [$roleName] permissions can do actions [$actions] with topics [$topicNames]")
            rolePolicies    <- awsIamService.getRolePolicies(roleName)
            kafkaTopicArns  <- topicNames.traverse(convertToTopicArn)
            isAllowed       <- awsIamService.allowedToDoActionsWithTopic(actions, kafkaTopicArns, rolePolicies)
            _               <- Logger[F].info(s"AwsSecurityService: Role's [$roleName] permissions ${if(!isAllowed) "do not" else ""} allow to do actions [$actions] with topics [$topicNames]")
          } yield isAllowed

        override def addAllTopicPermissionsPolicy(topicName: String, roleName: RoleName): F[Unit] =
          for {
            _ <- Logger[F].info(s"AwsSecurityService: Going to add admin policy for topic [$topicName] and role [$roleName]")
            allPermissionsPolicyForTopic <- getAllPermissionsPolicyForTopic(topicName)
            _ <- awsIamService.addRolePolicy(
              roleName,
              getAllTopicPermissionsPolicyName(topicName),
              allPermissionsPolicyForTopic
            )
          } yield ()

        override def deleteAllTopicPermissionsPolicy(topicName: String, roleName: RoleName): F[Unit] =
          Logger[F].info(s"AwsSecurityService: Going to delete admin permissions for topic [$topicName] and role [$roleName]") *>
            awsIamService.deleteRolePolicy(roleName, getAllTopicPermissionsPolicyName(topicName))

        override def deleteAllTopicPermissionsPolicy(topicNames: Vector[String], roleName: RoleName): F[Unit] =
          Logger[F].info(s"AwsSecurityService: Going to delete admin permissions for topics [$topicNames] and role [$roleName]") *>
            topicNames.traverse_(topicName => awsIamService.deleteRolePolicy(roleName, getAllTopicPermissionsPolicyName(topicName)))

        private def convertToTopicArn(topicName: String): F[Arn] =
          eff.fromOption(awsConfig.mskClusterArn, MskClusterArnNotDefined).map { mskClusterArn =>
            Arn.fromString(mskClusterArn.replace(":cluster/", ":topic/") ++ s"/$topicName")
          }

        private def getAllPermissionsPolicyForTopic(topicName: String): F[Policy] =
          convertToTopicArn(topicName).map { topicArn =>
            val actions: Vector[Action] = Vector(
              AwsIamPolicyAction.KafkaAction.CustomAction("*Topic*"),
              AwsIamPolicyAction.KafkaAction.WriteData,
              AwsIamPolicyAction.KafkaAction.ReadData
            )

            val resource = new Resource(topicArn.toString)

            val statement = new Statement(Statement.Effect.Allow)
            statement.setActions(actions.asJava)
            statement.setResources(Vector(resource).asJava)

            val policy = new Policy()
            policy.setStatements(Vector(statement).asJava)
            policy
          }

        private def getAllTopicPermissionsPolicyName(topicName: String) = PolicyName(s"internal-$topicName-admin-policy")
      }
    }
  }
}

