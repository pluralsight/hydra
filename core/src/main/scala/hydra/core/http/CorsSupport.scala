package hydra.core.http

/**
  * Created by alexsilva on 3/28/17.
  */
import ch.megard.akka.http.cors.scaladsl.settings.CorsSettings
import hydra.common.config.ConfigSupport

import scala.collection.immutable

trait CorsSupport extends ConfigSupport {

  val settings = CorsSettings.defaultSettings
    .withAllowCredentials(false)
    .withExposedHeaders(immutable.Seq("Link"))
}
