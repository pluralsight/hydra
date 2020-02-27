package hydra.ingest.app

import akka.actor.ActorSystem
import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import com.typesafe.config.ConfigFactory
import configs.syntax._
import hydra.common.logging.LoggingAdapter
import hydra.ingest.bootstrap.BootstrappingSupport
import hydra.ingest.modules.{Algebras, Bootstrap, Programs}
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import kamon.Kamon
import kamon.prometheus.PrometheusReporter

import scala.concurrent.ExecutionContext.Implicits.global

// $COVERAGE-OFF$Disabling highlighting by default until a workaround for https://issues.scala-lang.org/browse/SI-8596 is found
object Main extends IOApp with BootstrappingSupport with LoggingAdapter {

  private implicit val system: ActorSystem = containerService.system

  private implicit val catsLogger: SelfAwareStructuredLogger[IO] =
    Slf4jLogger.getLogger[IO]

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

  private val mainProgram = AppConfig.appConfig.load[IO].flatMap { config =>
    val ingestActorSelection = system.actorSelection(
      path = ConfigFactory.load().getString("hydra.kafka-ingestor-path")
    )
    for {
      _ <- oldBoostrap
      algebras <- Algebras
        .make[IO](config.createTopicConfig, ingestActorSelection)
      programs <- Programs.make[IO](config, algebras)
      bootstrap <- Bootstrap
        .make[IO](programs.createTopic, config.v2MetadataTopicConfig)
      _ <- bootstrap.bootstrapAll
    } yield ()
  }

  override def run(args: List[String]): IO[ExitCode] = {
    mainProgram.as(ExitCode.Success)
  }

}

// $COVERAGE-ON
