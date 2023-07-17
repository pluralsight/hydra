package hydra.core.http.security

import cats.effect.Sync
import cats.syntax.all._
import akka.http.scaladsl.server.Directives.optionalHeaderValueByName
import akka.http.scaladsl.server.directives.BasicDirectives._
import akka.http.scaladsl.server.directives.FutureDirectives.onComplete
import akka.http.scaladsl.server.directives.RouteDirectives._
import akka.http.scaladsl.server.{AuthorizationFailedRejection, Directive1}
import hydra.common.util.Futurable
import hydra.core.http.security.entity.{AwsConfig, AwsIamError}
import security._

import scala.util.{Failure, Success}

class AccessControlService[F[_]: Sync: Futurable](awsSecurityService: AwsSecurityService[F], awsConfig: AwsConfig) {

  def mskAuth(actions: AwsIamPolicyAction.KafkaAction*): Directive1[Option[RoleName]] = doMskAuth(Vector.empty, actions)

  def mskAuth(topicName: String, actions: AwsIamPolicyAction.KafkaAction*): Directive1[Option[RoleName]] = doMskAuth(Vector(topicName), actions)

  def mskAuth(topicNames: Vector[String], actions: AwsIamPolicyAction.KafkaAction*): Directive1[Option[RoleName]] = doMskAuth(topicNames, actions)

  private def doMskAuth(topics: Vector[String],
                        actions: Seq[AwsIamPolicyAction.KafkaAction]): Directive1[Option[RoleName]] =
    if (awsConfig.isAwsIamSecurityEnabled && awsConfig.mskClusterArn.isDefined) {
      (optionalHeaderValueByName("accesskeyid") & optionalHeaderValueByName("secretaccesskey") & optionalHeaderValueByName("sessiontoken")).tflatMap {
        case (accessKeyIdOpt, secretAccessKeyOpt, sessionTokenOpt) =>
          auth(topics, actions, accessKeyIdOpt.map(AccessKeyId), secretAccessKeyOpt.map(SecretAccessKey), sessionTokenOpt.map(SessionToken))
      }
    } else {
      provide(None): Directive1[Option[RoleName]]
    }

  private def auth(topics: Vector[String],
                   actions: Seq[AwsIamPolicyAction.KafkaAction],
                   accessKeyIdOpt: Option[AccessKeyId],
                   secretAccessKeyOpt: Option[SecretAccessKey],
                   sessionTokenOpt: Option[SessionToken]): Directive1[Option[RoleName]] = {
    def auth = for {
      roleName  <- awsSecurityService.getAssumedRoleName(accessKeyIdOpt, secretAccessKeyOpt, sessionTokenOpt)
      isAllowed <- awsSecurityService.isAssumedRoleAllowsToDoActions(actions, topics, roleName)
    } yield (roleName, isAllowed)

    // use this after authorization will be mandatory
    //    onComplete(Futurable[F].unsafeToFuture(auth)).flatMap {
    //      case Success((roleName, isAllowedActions)) if isAllowedActions  => provide(Some(roleName)): Directive1[Option[RoleName]]
    //      case Success((_, isAllowedActions)) if !isAllowedActions        => reject(AuthorizationFailedRejection): Directive1[Option[RoleName]]
    //      case Failure(exception)                                         => failWith(AwsIamError.AuthorizationServiceError(exception)): Directive1[Option[RoleName]]
    //    }

    //remove it after authorization will be mandatory
    onComplete(Futurable[F].unsafeToFuture(if (accessKeyIdOpt.isDefined && secretAccessKeyOpt.isDefined) auth.map(result => (Some(result._1), result._2)) else Sync[F].pure(None, true))).flatMap {
      case Success((Some(roleName), isAllowedActions)) if isAllowedActions  => provide(Some(roleName)): Directive1[Option[RoleName]]
      case Success((None, isAllowedActions)) if isAllowedActions            => provide(None): Directive1[Option[RoleName]]
      case Success((_, isAllowedActions)) if !isAllowedActions              => reject(AuthorizationFailedRejection): Directive1[Option[RoleName]]
      case Failure(exception)                                               => failWith(AwsIamError.AuthorizationServiceError(exception)): Directive1[Option[RoleName]]
    }
  }
}
