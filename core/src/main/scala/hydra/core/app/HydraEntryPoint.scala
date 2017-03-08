package hydra.core.app

import akka.actor.Props
import akka.event.slf4j.SLF4JLogging
import com.github.vonnagy.service.container.{ContainerBuilder, MissingConfigException}
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import com.github.vonnagy.service.container.service.ContainerService
import com.typesafe.config.{Config, ConfigFactory}
import configs.syntax._
import hydra.core.extensions.HydraExtensionListener

/**
  * Created by alexsilva on 2/24/17.
  */
trait HydraEntryPoint extends App with SLF4JLogging {

  type ENDPOINT = Class[_ <: RoutedEndpoints]

  def moduleName: String

  def config: Config

  def services: Seq[(String, Props)]

  def extensions = config.get[Config]("extensions").valueOrElse(ConfigFactory.empty)

  lazy val endpoints = config.get[List[String]](s"$moduleName.endpoints").valueOrElse(Seq.empty)
    .map(Class.forName(_).asInstanceOf[ENDPOINT])

  def buildContainer(fallbackCfg: Option[Config] = None): ContainerService = {
    val builder = ContainerBuilder()
      .withConfig(fallbackCfg.map(config.withFallback(_)).getOrElse(config))
      .withRoutes(endpoints: _*)
      .withActors(services: _*)
      .withListeners(new HydraExtensionListener(moduleName, extensions))

    builder.build
  }

  def validateConfig(paths: String*) = {
    paths.foreach { path =>
      if (!config.hasPath(path)) {
        throw new MissingConfigException(s"Missing required config property: '$path'.")
      }
    }
  }

}
