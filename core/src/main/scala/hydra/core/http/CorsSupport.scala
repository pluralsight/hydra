package hydra.core.http

/**
  * Created by alexsilva on 3/28/17.
  */

import ch.megard.akka.http.cors.CorsSettings
import hydra.common.config.ConfigSupport

trait CorsSupport extends ConfigSupport {
  val settings = CorsSettings.defaultSettings.copy(allowGenericHttpRequests = true)
}


