package hydra.core.http.security

import cats.effect.Sync
import cats.syntax.all._
import hydra.core.http.security.entity.AwsIamError
import org.typelevel.log4cats.Logger
import software.amazon.awssdk.arns.Arn
import software.amazon.awssdk.auth.credentials.{AwsSessionCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sts.StsClient
import security._

trait AwsStsClient[F[_]] {
  def getCallerIdentityArn(accessKeyId: AccessKeyId, secretAccessKey: SecretAccessKey, sessionTokenOpt: SessionToken): F[Arn]
}

object AwsStsClient {
  def make[F[_]: Logger](implicit eff: Sync[F]): F[AwsStsClient[F]] = {
    eff.delay {
      new AwsStsClient[F] {
        override def getCallerIdentityArn(accessKeyId: AccessKeyId,
                                          secretAccessKey: SecretAccessKey,
                                          sessionToken: SessionToken): F[Arn] = {
          val credentials = AwsSessionCredentials.create(accessKeyId.value, secretAccessKey.value, sessionToken.value)
          val credentialsProvider = StaticCredentialsProvider.create(credentials)

          val stsClient = StsClient.builder().region(IamGlobalRegion).credentialsProvider(credentialsProvider).build()

          eff.delay(stsClient.getCallerIdentity)
            .map(callerInfo => Arn.fromString(callerInfo.arn()))
            .handleErrorWith(e => Logger[F].error(e)(s"AwsStsClient: Error on getting caller identity by tokens.") *> e.raiseError)
            .adaptError { case _ => AwsIamError.CannotGetCallerIdentity }
        }
      }
    }
  }

  private val IamGlobalRegion = Region.AWS_GLOBAL

}
