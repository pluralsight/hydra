package hydra.core.app

import java.lang.reflect.Modifier

import akka.actor.Props
import com.github.vonnagy.service.container.ContainerBuilder
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import com.github.vonnagy.service.container.listener.ContainerLifecycleListener
import com.github.vonnagy.service.container.service.ContainerService
import hydra.common.config.ConfigSupport
import hydra.common.reflect.ReflectionUtils
import hydra.core.extensions.HydraExtensionListener
import org.reflections.Reflections
import org.reflections.scanners.SubTypesScanner

import scala.util.Try

trait ServiceLoader extends ConfigSupport {
  private val reflections = new Reflections("hydra", new SubTypesScanner)

  import configs.syntax._

  import scala.collection.JavaConverters._

  type ENDPOINT = Class[_ <: RoutedEndpoints]

  val serviceProviders = reflections.getSubTypesOf(classOf[ServiceProvider])
    .asScala.filterNot(c => Modifier.isAbstract(c.getModifiers)).toSeq

  val services: Seq[(String, Props)] = serviceProviders.flatMap { clz =>
    Try(ReflectionUtils.getObjectInstance(clz)).map(_.services)
      .getOrElse(clz.newInstance().services)
  }

  lazy val endpoints = rootConfig.get[List[String]](s"$applicationName.endpoints").valueOrElse(Seq.empty)
    .map(Class.forName(_).asInstanceOf[ENDPOINT])

  lazy val listeners = rootConfig.get[List[String]](s"$applicationName.listeners").valueOrElse(Seq.empty)
    .map(Class.forName(_).asInstanceOf[ContainerLifecycleListener])

  def buildContainer(): ContainerService = {
    val builder = ContainerBuilder()
      .withConfig(rootConfig)
      .withRoutes(endpoints: _*)
      .withActors(services: _*)
      .withListeners(new HydraExtensionListener(applicationConfig) +: listeners: _*)
      .withName(applicationName)

    builder.build
  }

  buildContainer().start()
}
