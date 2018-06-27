package hydra.core.bootstrap

import java.lang.reflect.Modifier

import akka.actor.Props
import akka.management.AkkaManagement
import akka.management.cluster.bootstrap.ClusterBootstrap
import com.github.vonnagy.service.container.ContainerBuilder
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import com.github.vonnagy.service.container.listener.ContainerLifecycleListener
import com.github.vonnagy.service.container.service.ContainerService
import com.pluralsight.hydra.reflect.DoNotScan
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.common.reflect.{ComponentInstantiator, ReflectionUtils}

import scala.util.Try

trait BootstrappingSupport extends ConfigSupport with LoggingAdapter {

  import ReflectionsWrapper._

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
    val container = ContainerBuilder()
      .withConfig(rootConfig)
      .withRoutes(endpoints: _*)
      .withActors(services: _*)
      .withListeners(listeners: _*)
      .withName(applicationName).build

    AkkaManagement(container.system).start()
    ClusterBootstrap(container.system).start()
    container
  }
}
