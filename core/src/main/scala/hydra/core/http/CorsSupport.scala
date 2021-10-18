package hydra.core.http

/**
  * Created by alexsilva on 3/28/17.
  */
import akka.http.scaladsl.model.headers.HttpOrigin
import akka.http.scaladsl.model.{HttpMethod, HttpMethods}
import ch.megard.akka.http.cors.scaladsl.model.HttpOriginMatcher
import ch.megard.akka.http.cors.scaladsl.settings.CorsSettings
import hydra.common.config.ConfigSupport

import scala.collection.immutable

trait CorsSupport extends ConfigSupport {

  private val allowedMethodKeys = scala.collection.immutable.Seq("GET", "POST", "PUT", "HEAD", "OPTIONS", "DELETE")
  private val allowedMethods =  allowedMethodKeys.map(k => HttpMethods.getForKey(k).getOrElse(HttpMethod.custom(k)))

  private val dataExplorerWildcardString = "*.vnerd.com"

  val settings = CorsSettings.defaultSettings
    .withAllowCredentials(false)
    .withExposedHeaders(immutable.Seq("Link"))
    .withAllowedMethods(allowedMethods)
    .withAllowedOrigins(HttpOriginMatcher.apply(HttpOrigin(dataExplorerWildcardString)))
}
