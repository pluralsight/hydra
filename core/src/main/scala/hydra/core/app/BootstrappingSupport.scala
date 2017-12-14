package hydra.core.app

import java.lang.reflect.Modifier

import akka.actor.Props
import com.github.vonnagy.service.container.ContainerBuilder
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import com.github.vonnagy.service.container.listener.ContainerLifecycleListener
import com.github.vonnagy.service.container.service.ContainerService
import com.pluralsight.hydra.reflect.DoNotScan
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.common.reflect.{ComponentInstantiator, ReflectionUtils}
import org.reflections.Reflections
import org.reflections.scanners.SubTypesScanner

import scala.util.Try

trait BootstrappingSupport extends ConfigSupport with LoggingAdapter {
  private val reflections = new Reflections("hydra", new SubTypesScanner)

  import scala.collection.JavaConverters._
  import scala.util.control.Exception._

  private val exceptionLogger = handling(classOf[Exception]) by { ex =>
    log.error("Could not instantiate class.", ex); None
  }

  val serviceProviders = scanFor(classOf[ServiceProvider])

  def services: Seq[(String, Props)] = serviceProviders.flatMap { clz =>
    Try(ReflectionUtils.getObjectInstance(clz)).map(_.services)
      .getOrElse(clz.newInstance().services)
  }

  lazy val endpoints = scanFor(classOf[RoutedEndpoints])

  lazy val listeners = scanFor(classOf[ContainerLifecycleListener]).flatMap { clz =>
    exceptionLogger(Some(ComponentInstantiator.instantiate(clz, List(applicationConfig)).get))
  }

  private def scanFor[T](clazz: Class[T]): Seq[Class[_ <: T]] = {
    reflections.getSubTypesOf(clazz)
      .asScala
      .filterNot(c => Modifier.isAbstract(c.getModifiers))
      .filterNot(c => c.isAnnotationPresent(classOf[DoNotScan])).toSeq
  }

  def containerService: ContainerService = {
    log.info(s"The following services will be started: ${services.map(_._1).mkString(", ")}")
    ContainerBuilder()
      .withConfig(rootConfig)
      .withRoutes(endpoints: _*)
      .withActors(services: _*)
      .withListeners(listeners: _*)
      .withName(applicationName).build
  }
}
