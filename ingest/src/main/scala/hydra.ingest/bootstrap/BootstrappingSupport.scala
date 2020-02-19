package hydra.ingest.bootstrap

import java.lang.reflect.Modifier

import akka.actor.{ActorSystem, Props}
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
import hydra.core.bootstrap.{CreateTopicProgram, ReflectionsWrapper, ServiceProvider}
import hydra.kafka.endpoints.BootstrapEndpointV2
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import retry.{RetryPolicies, RetryPolicy}

import scala.concurrent.ExecutionContext
import scala.util.Try

class BootstrapEndpoints(implicit val system: ActorSystem, implicit val ec: ExecutionContext) extends RoutedEndpoints {

  private implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)
  private implicit val logger: Logger[IO] = Slf4jLogger.getLogger

  private val schemaRegistry = SchemaRegistry.live[IO](ConfigFactory.load().getString("hydra.schema.registry.url"), 100).unsafeRunSync()

  private val bootstrapV2Endpoint = {
    val retryPolicy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
    new BootstrapEndpointV2(new CreateTopicProgram[IO](schemaRegistry, retryPolicy))
  }

  override def route: Route = bootstrapV2Endpoint.route
}

trait BootstrappingSupport extends ConfigSupport with LoggingAdapter {

  import ReflectionsWrapper._

  import scala.collection.JavaConverters._
  import scala.util.control.Exception._

  private def scanFor[T](clazz: Class[T]): Seq[Class[_ <: T]] = {
    reflections.getSubTypesOf(clazz)
      .asScala
      .filterNot(c => Modifier.isAbstract(c.getModifiers))
      .filterNot(c => c.isAnnotationPresent(classOf[DoNotScan])).toSeq
  }

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
