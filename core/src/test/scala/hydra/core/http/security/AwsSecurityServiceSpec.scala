package hydra.core.http.security

import cats.effect._
import cats.syntax.all._
import cats.effect.IO
import com.amazonaws.auth.policy.Policy
import hydra.common.validation.ValidationError.ValidationCombinedErrors
import hydra.core.IOSuite
import hydra.core.http.security.AwsIamPolicyAction.KafkaAction
import hydra.core.http.security.entity.AwsConfig
import hydra.core.http.security.entity.AwsIamError.{MissingAccessKeyIdField, MissingSecretAccessKey, MissingSessionToken}
import hydra.core.http.security.entity.SecurityValidationError.{EmptyAccessKeyId, EmptySecretAccessKey, EmptySessionToken}
import hydra.core.http.security.security.{AccessKeyId, PolicyName, RoleName, SecretAccessKey, SessionToken}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.arns.Arn

class AwsSecurityServiceSpec extends AsyncFreeSpec with AsyncMockFactory with Matchers with IOSuite {
  import AwsSecurityServiceSpec._

  "AwsSecurityService" - {
    "should return assumed role name when tokens are valid" in {
      val iamService = mock[AwsIamService[IO]]
      (iamService.getRoleName _).expects(accessKeyId, secretAccessKey, sessionToken).returns(IO.pure(roleName))

      for {
        service <- AwsSecurityService.make(iamService, awsConfig)
        actualRoleName <- service.getAssumedRoleName(accessKeyId.some, secretAccessKey.some, sessionToken.some)
      } yield actualRoleName shouldBe roleName
    }

    "should return MissingAccessKeyIdField error when accessKeyId is not provided" in {
      val iamService = mock[AwsIamService[IO]]
      (iamService.getRoleName _).expects(*, *, *).never()

      val result = for {
        service <- AwsSecurityService.make(iamService, awsConfig)
        _ <- service.getAssumedRoleName(none, secretAccessKey.some, sessionToken.some)
      } yield ()

      result.attempt.map(_ shouldBe MissingAccessKeyIdField.asLeft)
    }

    "should return MissingSecretAccessKey error when secretAccessKey is not provided" in {
      val iamService = mock[AwsIamService[IO]]
      (iamService.getRoleName _).expects(*, *, *).never()

      val result = for {
        service <- AwsSecurityService.make(iamService, awsConfig)
        _ <- service.getAssumedRoleName(accessKeyId.some, none, sessionToken.some)
      } yield ()

      result.attempt.map(_ shouldBe MissingSecretAccessKey.asLeft)
    }

    "should return MissingSessionToken error when sessionToken is not provided" in {
      val iamService = mock[AwsIamService[IO]]
      (iamService.getRoleName _).expects(*, *, *).never()

      val result = for {
        service <- AwsSecurityService.make(iamService, awsConfig)
        _ <- service.getAssumedRoleName(accessKeyId.some, secretAccessKey.some, none)
      } yield ()

      result.attempt.map(_ shouldBe MissingSessionToken.asLeft)
    }


    "should return error about empty keys when keys are empty" in {
      val iamService = mock[AwsIamService[IO]]
      (iamService.getRoleName _).expects(*, *, *).never()

      val result = for {
        service <- AwsSecurityService.make(iamService, awsConfig)
        _ <- service.getAssumedRoleName(AccessKeyId("  ").some, SecretAccessKey("").some, SessionToken("").some)
      } yield ()

      result.attempt.map(_ shouldBe ValidationCombinedErrors(List(EmptyAccessKeyId.message, EmptySecretAccessKey.message, EmptySessionToken.message)).asLeft)
    }

    "should return true when actions with topic are allowed by iam" in {
      val actions = Seq(KafkaAction.CreateTopic)
      val iamService = mock[AwsIamService[IO]]
      (iamService.getRolePolicies _).expects(roleName).returns(IO.pure(policies))
      (iamService.allowedToDoActionsWithTopic _).expects(actions, Vector(topicArn), policies).returns(IO.pure(true))

      for {
        service <- AwsSecurityService.make(iamService, awsConfig)
        isAllowed <- service.isAssumedRoleAllowsToDoActions(actions, Vector(topicName), roleName)
      } yield isAllowed shouldBe true
    }

    "should return false when actions with topic are not allowed by iam" in {
      val actions = Seq(KafkaAction.CreateTopic)
      val iamService = mock[AwsIamService[IO]]
      (iamService.getRolePolicies _).expects(roleName).returns(IO.pure(policies))
      (iamService.allowedToDoActionsWithTopic _).expects(actions, Vector(topicArn), policies).returns(IO.pure(false))

      for {
        service <- AwsSecurityService.make(iamService, awsConfig)
        isAllowed <- service.isAssumedRoleAllowsToDoActions(actions, Vector(topicName), roleName)
      } yield isAllowed shouldBe false
    }

    "should add policy to role with all permissions" in {
      val iamService = mock[AwsIamService[IO]]
      (iamService.addRolePolicy _).expects(roleName, internalPolicyNameForTopic, *).returns(IO.unit).once()

      val result = for {
        service <- AwsSecurityService.make(iamService, awsConfig)
        _ <- service.addAllTopicPermissionsPolicy(topicName, roleName)
      } yield ()

      result.attempt.map(_.isRight shouldBe true)
    }

    "should delete all permissions policy for a topic" in {
      val iamService = mock[AwsIamService[IO]]
      (iamService.deleteRolePolicy _).expects(roleName, internalPolicyNameForTopic).returns(IO.unit).once()

      val result = for {
        service <- AwsSecurityService.make(iamService, awsConfig)
        _ <- service.deleteAllTopicPermissionsPolicy(topicName, roleName)
      } yield ()

      result.attempt.map(_.isRight shouldBe true)
    }

    "should delete all permissions policy for list of topics" in {
      val iamService = mock[AwsIamService[IO]]
      (iamService.deleteRolePolicy _).expects(roleName, internalPolicyNameForTopic).returns(IO.unit).once()
      (iamService.deleteRolePolicy _).expects(roleName, internalPolicyNameForTopic2).returns(IO.unit).once()

      val result = for {
        service <- AwsSecurityService.make(iamService, awsConfig)
        _ <- service.deleteAllTopicPermissionsPolicy(Vector(topicName, topicName2), roleName)
      } yield ()

      result.attempt.map(_.isRight shouldBe true)
    }

  }
}

object AwsSecurityServiceSpec {
  val mskClusterArn = "arn:aws:kafka:us-east-1:0123456789012:cluster/MyTestCluster/abcd1234-0123-abcd-5678-1234abcd-1"
  val awsConfig = AwsConfig(mskClusterArn, true)

  val accessKeyId = AccessKeyId("someaccesskey")
  val secretAccessKey = SecretAccessKey("somesecretaccesskey")
  val sessionToken = SessionToken("somesessiontoken")

  val roleName = RoleName("somerole")

  val topicName = "sometopicname"
  val internalPolicyNameForTopic = PolicyName(s"internal-$topicName-admin-policy")
  val topicArn = Arn.fromString(s"arn:aws:kafka:us-east-1:0123456789012:topic/MyTestCluster/abcd1234-0123-abcd-5678-1234abcd-1/$topicName")

  val topicName2 = "sometopicname"
  val internalPolicyNameForTopic2 = PolicyName(s"internal-$topicName2-admin-policy")

  val policy1 = new Policy("id1")
  val policy2 = new Policy("id2")
  val policy3 = new Policy("id3")

  val policies = Vector(policy1, policy2, policy3)
}