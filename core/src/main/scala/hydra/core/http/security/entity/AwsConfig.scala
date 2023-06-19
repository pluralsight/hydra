package hydra.core.http.security.entity

final case class AwsConfig(mskClusterArn: Option[String], isAwsIamSecurityEnabled: Boolean)
