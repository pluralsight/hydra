package hydra.core.app

import akka.actor.Props
import akka.event.slf4j.SLF4JLogging
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import com.github.vonnagy.service.container.listener.ContainerLifecycleListener
import com.github.vonnagy.service.container.service.ContainerService
import com.github.vonnagy.service.container.{ContainerBuilder, MissingConfigException}
import com.typesafe.config.{Config, ConfigFactory}
import configs.syntax._
import hydra.common.config.ConfigSupport
import hydra.core.extensions.HydraExtensionListener

/**
  * Created by alexsilva on 2/24/17.
  */
trait HydraEntryPoint extends App with SLF4JLogging with ConfigSupport {

  type ENDPOINT = Class[_ <: RoutedEndpoints]

  def moduleName: String

  def config: Config

  def services: Seq[(String, Props)]

  def containerName: String = s"$applicationName-$moduleName"

  def extensions = config.get[Config](s"$applicationName.extensions").valueOrElse(ConfigFactory.empty)

  def listeners: Seq[ContainerLifecycleListener] = Seq.empty

  lazy val endpoints = config.get[List[String]](s"$applicationName.$moduleName.endpoints").valueOrElse(Seq.empty)
    .map(Class.forName(_).asInstanceOf[ENDPOINT])

  def buildContainer(): ContainerService = {
    val hydraExts = if (extensions.isEmpty) Seq.empty else Seq(new HydraExtensionListener(moduleName, extensions))
    val builder = ContainerBuilder()
      .withConfig(config)
      .withRoutes(endpoints: _*)
      .withActors(services: _*)
      .withListeners(hydraExts ++ listeners: _*)
      .withName(containerName)

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
