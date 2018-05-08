package hydra.common

import com.typesafe.config.Config
import configs.syntax._
import hydra.common.auth.{HydraAuthenticator, NoSecurityAuthenticator}
import hydra.common.config.ConfigSupport

class Settings(config: Config) {
  val Authenticator: HydraAuthenticator =
    config.get[String]("http.authenticator")
      .map(c => Class.forName(c).newInstance().asInstanceOf[HydraAuthenticator])
      .valueOrElse(new NoSecurityAuthenticator)
}

object Settings extends ConfigSupport {
  val HydraSettings = new Settings(applicationConfig)
}
