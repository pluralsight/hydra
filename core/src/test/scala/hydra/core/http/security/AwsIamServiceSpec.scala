package hydra.core.http.security

import cats.effect._
import cats.syntax.all._
import com.amazonaws.auth.policy.Policy
import hydra.core.IOSuite
import hydra.core.http.security.AwsIamPolicyAction.KafkaAction
import hydra.core.http.security.entity.AwsIamError
import hydra.core.http.security.security.{AccessKeyId, PolicyName, RoleName, SecretAccessKey, SessionToken, VersionId}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.arns.Arn
import software.amazon.awssdk.services.iam.model.AttachedPolicy
import software.amazon.awssdk.services.iam.model.{Policy => ManagedPolicy}

class AwsIamServiceSpec extends AsyncFreeSpec with AsyncMockFactory with Matchers with IOSuite {
  import AwsIamServiceSpec._

  "AwsIamService" - {
    "should retrieve RoleName by tokens when tokens were given by assume role" in {
      val awsStsClientMock = mock[AwsStsClient[IO]]

      (awsStsClientMock.getCallerIdentityArn _)
        .expects(accessKeyId, secretAccessKey, sessionToken)
        .returns(IO.pure(assumedRoleArn))

      for {
        service <- AwsIamService.make(mock[AwsIamClient[IO]], awsStsClientMock)
        actualRoleName <- service.getRoleName(accessKeyId, secretAccessKey, sessionToken)
      } yield {
        actualRoleName shouldBe roleName
      }
    }

    "should return CannotRetrieveRoleName when role name cannot be retrieved" in {
      val awsStsClientMock = mock[AwsStsClient[IO]]

      (awsStsClientMock.getCallerIdentityArn _)
        .expects(accessKeyId, secretAccessKey, sessionToken)
        .returns(IO.pure(federatedUserArn))

      val result = for {
        service <- AwsIamService.make(mock[AwsIamClient[IO]], awsStsClientMock)
        _ <- service.getRoleName(accessKeyId, secretAccessKey, sessionToken)
      } yield ()

      result.attempt.map(_ shouldBe AwsIamError.CannotRetrieveRoleName(federatedUserArn).asLeft)
    }

    "should return all inline policies when role has only inline policies" in {
      val iamClient = mock[AwsIamClient[IO]]

      (iamClient.listRolePoliciesNames _).expects(roleName).returns(IO.pure(Vector(inlinePolicyName1.value, inlinePolicyName2.value, inlinePolicyName3.value)))
      (iamClient.getRolePolicy _).expects(roleName, inlinePolicyName1).returns(IO.pure(inlinePolicy1.some))
      (iamClient.getRolePolicy _).expects(roleName, inlinePolicyName2).returns(IO.pure(inlinePolicy2.some))
      (iamClient.getRolePolicy _).expects(roleName, inlinePolicyName3).returns(IO.pure(none))

      (iamClient.listAttachedRolePolicies _).expects(roleName).returns(IO.pure(Vector.empty))
      (iamClient.getPolicy _).expects(*).never()
      (iamClient.getPolicyVersion _).expects(*, *).never()

      for {
        service <- AwsIamService.make(iamClient, mock[AwsStsClient[IO]])
        actualRolePolicies <- service.getRolePolicies(roleName)
      } yield {
        actualRolePolicies shouldBe Vector(inlinePolicy1, inlinePolicy2)
      }
    }

    "should return all managed policies when role has only managed policies" in {
      val iamClient = mock[AwsIamClient[IO]]

      (iamClient.listRolePoliciesNames _).expects(roleName).returns(IO.pure(Vector.empty))
      (iamClient.getRolePolicy _).expects(*, *).never()

      (iamClient.listAttachedRolePolicies _).expects(roleName).returns(IO.pure(Vector(attachedPolicy1, attachedPolicy2)))
      (iamClient.getPolicy _).expects(managedPolicyArn1).returns(IO.pure(managedPolicy1.some))
      (iamClient.getPolicy _).expects(managedPolicyArn2).returns(IO.pure(managedPolicy2.some))
      (iamClient.getPolicyVersion _).expects(managedPolicyArn1, VersionId(managedPolicyVersion1)).returns(IO.pure(managedDefaultPolicy1.some))
      (iamClient.getPolicyVersion _).expects(managedPolicyArn2, VersionId(managedPolicyVersion2)).returns(IO.pure(managedDefaultPolicy2.some))

      for {
        service <- AwsIamService.make(iamClient, mock[AwsStsClient[IO]])
        actualRolePolicies <- service.getRolePolicies(roleName)
      } yield {
        actualRolePolicies shouldBe Vector(managedDefaultPolicy1, managedDefaultPolicy2)
      }
    }

    "should return inline and managed policies" in {
      val iamClient = mock[AwsIamClient[IO]]

      (iamClient.listRolePoliciesNames _).expects(roleName).returns(IO.pure(Vector(inlinePolicyName1.value, inlinePolicyName2.value, inlinePolicyName3.value)))
      (iamClient.getRolePolicy _).expects(roleName, inlinePolicyName1).returns(IO.pure(inlinePolicy1.some))
      (iamClient.getRolePolicy _).expects(roleName, inlinePolicyName2).returns(IO.pure(inlinePolicy2.some))
      (iamClient.getRolePolicy _).expects(roleName, inlinePolicyName3).returns(IO.pure(none))

      (iamClient.listAttachedRolePolicies _).expects(roleName).returns(IO.pure(Vector(attachedPolicy1, attachedPolicy2)))
      (iamClient.getPolicy _).expects(managedPolicyArn1).returns(IO.pure(managedPolicy1.some))
      (iamClient.getPolicy _).expects(managedPolicyArn2).returns(IO.pure(managedPolicy2.some))
      (iamClient.getPolicyVersion _).expects(managedPolicyArn1, VersionId(managedPolicyVersion1)).returns(IO.pure(managedDefaultPolicy1.some))
      (iamClient.getPolicyVersion _).expects(managedPolicyArn2, VersionId(managedPolicyVersion2)).returns(IO.pure(managedDefaultPolicy2.some))

      for {
        service <- AwsIamService.make(iamClient, mock[AwsStsClient[IO]])
        actualRolePolicies <- service.getRolePolicies(roleName)
      } yield {
        actualRolePolicies shouldBe Vector(inlinePolicy1, inlinePolicy2, managedDefaultPolicy1, managedDefaultPolicy2)
      }
    }

    "should return empty list when role doesn't have policies" in {
      val iamClient = mock[AwsIamClient[IO]]

      (iamClient.listRolePoliciesNames _).expects(roleName).returns(IO.pure(Vector.empty))
      (iamClient.getRolePolicy _).expects(*, *).never()
      (iamClient.listAttachedRolePolicies _).expects(roleName).returns(IO.pure(Vector.empty))
      (iamClient.getPolicy _).expects(*).never()
      (iamClient.getPolicyVersion _).expects(*, *).never()

      for {
        service <- AwsIamService.make(iamClient, mock[AwsStsClient[IO]])
        actualRolePolicies <- service.getRolePolicies(roleName)
      } yield {
        actualRolePolicies.isEmpty shouldBe true
      }
    }

    "should add policy to role" in {
      val iamClient = mock[AwsIamClient[IO]]
      (iamClient.putRolePolicy _).expects(roleName, inlinePolicyName1, inlinePolicy1).returns(IO.unit).once()

      val result = for {
        service <- AwsIamService.make(iamClient, mock[AwsStsClient[IO]])
        _ <- service.addRolePolicy(roleName, inlinePolicyName1, inlinePolicy1)
      } yield ()

      result.attempt.map(_.isRight shouldBe true)
    }

    "should delete rol policy" in {
      val iamClient = mock[AwsIamClient[IO]]
      (iamClient.deleteRolePolicy _).expects(roleName, inlinePolicyName1).returns(IO.unit).once()

      val result = for {
        service <- AwsIamService.make(iamClient, mock[AwsStsClient[IO]])
        _ <- service.deleteRolePolicy(roleName, inlinePolicyName1)
      } yield ()

      result.attempt.map(_.isRight shouldBe true)
    }

    "should return true if actions with resources are allowed" in {
      val iamClient = mock[AwsIamClient[IO]]
      (iamClient.allowedToDoActionsWithTopic _).expects(actions, topicsArns, policies).returns(IO.pure(true))

      for {
        service <- AwsIamService.make(iamClient, mock[AwsStsClient[IO]])
        result <- service.allowedToDoActionsWithTopic(actions, topicsArns, policies)
      } yield result shouldBe true
    }

    "should return false if actions with resources are not allowed" in {
      val iamClient = mock[AwsIamClient[IO]]
      (iamClient.allowedToDoActionsWithTopic _).expects(actions, topicsArns, policies).returns(IO.pure(false))

      for {
        service <- AwsIamService.make(iamClient, mock[AwsStsClient[IO]])
        result <- service.allowedToDoActionsWithTopic(actions, topicsArns, policies)
      } yield result shouldBe false
    }
  }
}

object AwsIamServiceSpec {
  val accessKeyId = AccessKeyId("someaccesskey")
  val secretAccessKey = SecretAccessKey("somesecretaccesskey")
  val sessionToken = SessionToken("somesessiontoken")

  val roleNameStr = "SomeCoolRole"
  val roleName = RoleName(roleNameStr)
  val assumedRoleArn = Arn.fromString(s"arn:aws:sts::905274996831:assumed-role/$roleNameStr/AWSCLI-Session")
  val federatedUserArn = Arn.fromString("arn:aws:sts::123456789012:federated-user/Bob")

  val inlinePolicyName1 = PolicyName("iminlinepolicy1")
  val inlinePolicy1 = new Policy("id1")

  val inlinePolicyName2 = PolicyName("iminlinepolicy2")
  val inlinePolicy2 = new Policy("id2")

  val inlinePolicyName3 = PolicyName("iminlinepolicy3")

  val managedPolicyName1 = "immanagedpolicy1"
  val managedPolicyArn1 = Arn.fromString("arn:aws:iam::905274996831:policy/SomePolicy1")
  val managedPolicyVersion1 = "version1"
  val managedDefaultPolicy1 = new Policy("id3")

  val managedPolicyName2 = "immanagedpolicy2"
  val managedPolicyArn2 = Arn.fromString("arn:aws:iam::905274996831:policy/SomePolicy2")
  val managedPolicyVersion2 = "version2"
  val managedDefaultPolicy2 = new Policy("id4")

  val attachedPolicy1 = AttachedPolicy.builder().policyArn(managedPolicyArn1.toString).build()
  val attachedPolicy2 = AttachedPolicy.builder().policyArn(managedPolicyArn2.toString).build()

  val managedPolicy1 = ManagedPolicy.builder().defaultVersionId(managedPolicyVersion1).build()
  val managedPolicy2 = ManagedPolicy.builder().defaultVersionId(managedPolicyVersion2).build()

  val topicArn = Arn.fromString("arn:aws:kafka:us-east-1:0123456789012:topic/MyTestCluster/abcd1234-0123-abcd-5678-1234abcd-1/MyTopic")

  val actions = Seq(KafkaAction.CreateTopic)
  val topicsArns = Vector(topicArn)
  val policies = Vector(inlinePolicy1)
}
