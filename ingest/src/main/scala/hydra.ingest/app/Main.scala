package hydra.ingest.app

import cats.effect.{Blocker, ExitCode, IO, IOApp, Resource}
import configs.syntax._
import hydra.common.logging.LoggingAdapter
import hydra.core.bootstrap.BootstrappingSupport
import hydra.ingest.modules.{Algebras, Bootstrap, Programs}
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import kamon.Kamon
import kamon.prometheus.PrometheusReporter
import cats.implicits._

import scala.concurrent.ExecutionContext.Implicits.global

// $COVERAGE-OFF$Disabling highlighting by default until a workaround for https://issues.scala-lang.org/browse/SI-8596 is found
object Main extends IOApp with BootstrappingSupport with LoggingAdapter {

  private implicit val catsLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  private val blockerResource: Resource[IO, Blocker] = Blocker.apply[IO]

  private val oldBoostrap = IO(try {
    val enablePrometheus = applicationConfig
      .get[Boolean]("monitoring.prometheus.enable")
      .valueOrElse(false)
    if (enablePrometheus) {
      val module = new PrometheusReporter()
      Kamon.registerModule("MainModule", module)
    }

    containerService.start()
  } catch {
    case e: Throwable => {
      log.error("Unhandled exception.  Shutting down actor system.", e)
      Kamon
        .stopModules()
        .map(_ => containerService.shutdown())
        .onComplete(throw e)
    }
  })

  private val mainProgram = blockerResource.use(AppConfig.appConfig(_).load[IO]).flatMap { config =>
    for {
      algebras <- Algebras.make[IO](config.createTopicConfig.schemaRegistryConfig)
      programs <- Programs.make[IO](config.createTopicConfig, algebras)
      _ <- Bootstrap.make[IO](programs.createTopic, config.v2MetadataTopicConfig)
      _ <- oldBoostrap
    } yield ()
  }

  override def run(args: List[String]): IO[ExitCode] = {
    mainProgram.as(ExitCode.Success)
  }

}

// $COVERAGE-ON
