package hydra.core

import com.typesafe.config.Config
import configs.syntax._
import hydra.common.config.ConfigSupport
import hydra.core.auth.{HydraAuthenticator, NoSecurityAuthenticator}

class Settings(config: Config) {
  val IngestTopicName: String = "hydra-ingest"

  val Authenticator: HydraAuthenticator =
    config.get[String]("http.authenticator")
      .map(c => Class.forName(c).newInstance().asInstanceOf[HydraAuthenticator])
      .valueOrElse(new NoSecurityAuthenticator)
}


object Settings extends ConfigSupport {
  val HydraSettings = new Settings(applicationConfig)
}