package hydra.ingest.bootstrap

import java.lang.reflect.Modifier

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.RouteDirectives
import cats.effect.{ExitCode, IO, Resource, Timer}
import com.pluralsight.hydra.reflect.DoNotScan
import com.typesafe.config.ConfigFactory
import hydra.avro.registry.SchemaRegistry
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.common.reflect.ReflectionUtils
import hydra.core.bootstrap.{
  CreateTopicProgram,
  ReflectionsWrapper,
  ServiceProvider
}
import hydra.core.http.RouteSupport
import hydra.kafka.endpoints.BootstrapEndpointV2
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import retry.{RetryPolicies, RetryPolicy}
import cats.implicits._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Try

class BootstrapEndpoints(
    implicit val system: ActorSystem,
    implicit val ec: ExecutionContext
) extends RouteSupport {

  private implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)
  private implicit val logger: Logger[IO] = Slf4jLogger.getLogger

  private val schemaRegistryUrl =
    ConfigFactory.load().getString("hydra.schema.registry.url")

  private val schemaRegistry =
    SchemaRegistry.live[IO](schemaRegistryUrl, 100).unsafeRunSync()

  private val isBootstrapV2Enabled =
    ConfigFactory.load().getBoolean("hydra.v2.create-topic.enabled")

  private val bootstrapV2Endpoint = {
    if (isBootstrapV2Enabled) {
      val retryPolicy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      new BootstrapEndpointV2(
        new CreateTopicProgram[IO](schemaRegistry, retryPolicy)
      ).route
    } else {
      RouteDirectives.reject
    }
  }

  override def route: Route = bootstrapV2Endpoint
}

trait BootstrappingSupport extends ConfigSupport with LoggingAdapter {

  import scala.util.control.Exception._

  private val exceptionLogger = handling(classOf[Exception]) by { ex =>
    log.error("Could not instantiate class.", ex); None
  }

  getActorSystem.use { system =>
    buildProgram(system) *> IO.never.map(_ => ExitCode.Success)
  }

  private def buildProgram(system: ActorSystem): IO[Unit] = {
    implicit val actorSystem: ActorSystem = system
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    for {
      config <- IO(ConfigFactory.load)
      routes <- IO(RouteFactory.getRoutes())
      actors <- IO(ActorFactory.getActors())
    } yield ()
  }

  private def getActorSystem: Resource[IO, ActorSystem] = {
    val registerCoordinatedShutdown: ActorSystem => IO[Unit] = system =>
      IO(system.terminate())

    val system = for {
      config <- IO(ConfigFactory.load)
      system <- IO(ActorSystem("hydra", config))
    } yield system

    Resource.make(system)(registerCoordinatedShutdown)
  }
}
