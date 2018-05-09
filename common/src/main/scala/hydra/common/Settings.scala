package hydra.common

import com.typesafe.config.Config
import configs.syntax._
import hydra.common.auth.{HydraAuthenticator, NoSecurityAuthenticator}
import hydra.common.config.ConfigSupport

import scala.concurrent.duration._

class Settings(config: Config) {
  val IngestTopicName: String = "hydra-ingest"

  val Authenticator: HydraAuthenticator =
    config.get[String]("http.authenticator")
      .map(c => Class.forName(c).newInstance().asInstanceOf[HydraAuthenticator])
      .valueOrElse(new NoSecurityAuthenticator)

  val SchemaMetadataRefreshInterval = config.get[FiniteDuration]("schema.metadata.refresh.interval")
    .valueOrElse(1 minute)
}

object Settings extends ConfigSupport {
  val HydraSettings = new Settings(applicationConfig)
}
