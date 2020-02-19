package hydra.ingest.bootstrap

import java.lang.reflect.Modifier

import akka.ConfigurationException
import akka.actor.{ActorSystem, ExtendedActorSystem, Props}
import akka.http.scaladsl.server.Route
import cats.effect.{IO, Timer}
import com.github.vonnagy.service.container.ContainerBuilder
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import com.github.vonnagy.service.container.listener.ContainerLifecycleListener
import com.github.vonnagy.service.container.service.ContainerService
import com.pluralsight.hydra.reflect.DoNotScan
import com.typesafe.config.ConfigFactory
import hydra.avro.registry.SchemaRegistry
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.common.reflect.{ComponentInstantiator, ReflectionUtils}
import hydra.common.util.RoutedEndpointLookup
import hydra.core.bootstrap.{CreateTopicProgram, ReflectionsWrapper, ServiceProvider}
import hydra.kafka.endpoints.BootstrapEndpointV2
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import retry.{RetryPolicies, RetryPolicy}

import scala.concurrent.ExecutionContext
import scala.util.Try

class BootstrapEndpoints(implicit val system: ActorSystem, implicit val ec: ExecutionContext) extends RoutedEndpoints {
  import BootstrappingSupport._

  private implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)
  private implicit val logger: Logger[IO] = Slf4jLogger.getLogger

  private val schemaRegistry = SchemaRegistry.live[IO](ConfigFactory.load().getString("hydra.schema.registry.url"), 100).unsafeRunSync()

  private val bootstrapV2Endpoint = {
    val retryPolicy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
    new BootstrapEndpointV2(new CreateTopicProgram[IO](schemaRegistry, retryPolicy))
  }

  private lazy val allEndpointClasses = scanFor(classOf[RoutedEndpointLookup])

  private lazy val allEndpoints = {
    for {
      route <- allEndpointClasses
    } yield {
      val args = List(classOf[ActorSystem] -> system, classOf[ExecutionContext] -> ec)

      system.asInstanceOf[ExtendedActorSystem].dynamicAccess
        .createInstanceFor[RoutedEndpointLookup](route.getName, args).map({ route =>
          route
      }).recover({
        case e => throw new ConfigurationException(
          "RoutedEndpoints can't be loaded [" + route.getName +
            "] due to [" + e.toString + "]", e)
      }).get
    }
  }

  override def route: Route = allEndpoints.map(_.route).reduce(_ ~ _) ~ bootstrapV2Endpoint.route
}

trait BootstrappingSupport extends ConfigSupport with LoggingAdapter {

  import scala.util.control.Exception._
  import BootstrappingSupport._

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

object BootstrappingSupport {

  import ReflectionsWrapper._
  import scala.collection.JavaConverters._

  def scanFor[T](clazz: Class[T]): Seq[Class[_ <: T]] = {
    reflections.getSubTypesOf(clazz)
      .asScala
      .filterNot(c => Modifier.isAbstract(c.getModifiers))
      .filterNot(c => c.isAnnotationPresent(classOf[DoNotScan])).toSeq
  }

}
