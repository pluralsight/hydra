package hydra.core.app

import akka.actor.Props
import akka.event.slf4j.SLF4JLogging
import com.github.vonnagy.service.container.ContainerBuilder
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

  def applicationName: String = "hydra"

  def config: Config

  def services: Seq[(String, Props)]

  def extensions = config.get[Config](s"$applicationName.extensions").valueOrElse(ConfigFactory.empty)

  lazy val endpoints = config.get[List[String]](s"$applicationName.$moduleName.endpoints").valueOrElse(Seq.empty)
    .map(Class.forName(_).asInstanceOf[ENDPOINT])

  def beforeStart(builder: ContainerBuilder): ContainerBuilder = builder

  def buildContainer(): ContainerService = {

    val builder = ContainerBuilder()
      .withConfig(config)
      .withRoutes(endpoints: _*)
      .withActors(services: _*)
      .withListeners(new HydraExtensionListener(moduleName, extensions))

    beforeStart(builder).build
  }

  def doStart(): Unit = buildContainer().start()

}
