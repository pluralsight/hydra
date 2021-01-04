package hydra.ingest.app

import akka.actor.{Actor, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.server.Route
import cats.effect.{ExitCode, IO, IOApp, Resource}
import hydra.common.Settings
import hydra.common.config.ConfigSupport
import ConfigSupport._
import hydra.common.logging.LoggingAdapter
import hydra.ingest.bootstrap.ActorFactory
import hydra.ingest.modules.{Algebras, Bootstrap, Programs, Routes}
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import kamon.Kamon
import kamon.prometheus.PrometheusReporter

import scala.concurrent.ExecutionContext.Implicits.global

// $COVERAGE-OFF$Disabling highlighting by default until a workaround for https://issues.scala-lang.org/browse/SI-8596 is found
object Main extends IOApp with ConfigSupport with LoggingAdapter {

  private implicit val catsLogger: SelfAwareStructuredLogger[IO] =
    Slf4jLogger.getLogger[IO]

  private def getActorSystem: Resource[IO, ActorSystem] = {
    val registerCoordinatedShutdown: ActorSystem => IO[Unit] = system =>
      IO(system.terminate())
    val system = IO(ActorSystem("hydra", rootConfig))
    Resource.make(system)(registerCoordinatedShutdown)
  }

  private def report =
    IO({
      val enablePrometheus = applicationConfig
        .getBooleanOpt("monitoring.prometheus.enable")
        .getOrElse(false)
      if (enablePrometheus) {
        val module = new PrometheusReporter()
        Kamon.registerModule("MainModule", module)
      }
    })

  private def actorsIO()(implicit system: ActorSystem): IO[Unit] = {
    IO {
      class Service extends Actor {
        override def preStart(): Unit = {
          ActorFactory.getActors().foreach {
            case (name, props) =>
              context.actorOf(props, name)
          }
        }
        override def receive: Receive = {
          case _ => ()
        }
      }
      system.actorOf(Props[Service], "service")
    }
  }

  private def serverIO(routes: Routes[IO], settings: Settings)(
      implicit system: ActorSystem
  ): IO[ServerBinding] =
    for {
      r <- routes.routes
      server <- IO.fromFuture(
        IO(
          Http().bindAndHandle(r, settings.httpInterface, settings.httpPort)
        )
      )
    } yield server

  private def buildProgram()(implicit system: ActorSystem): IO[Unit] = {
    import scalacache.Mode
    implicit val mode: Mode[IO] = scalacache.CatsEffect.modes.async

    AppConfig.appConfig.load[IO].flatMap { config =>
      for {
        algebras <- Algebras
          .make[IO](config)
        programs <- Programs.make[IO](config, algebras)
        bootstrap <- Bootstrap
          .make[IO](programs.createTopic, config.v2MetadataTopicConfig, config.dvsConsumersTopicConfig, config.consumerOffsetsOffsetsTopicConfig)
        _ <- actorsIO()
        _ <- bootstrap.bootstrapAll
        routes <- Routes.make[IO](programs, algebras, config)
        _ <- report
        _ <- serverIO(routes, Settings.HydraSettings)
        _ <- if (config.consumerGroupsAlgebraConfig.consumerGroupsConsumerEnabled) {
          algebras.consumerGroups.startConsumer
        } else {
          IO.unit
        }
      } yield ()
    }
  }

  override def run(args: List[String]): IO[ExitCode] = {
    getActorSystem.use { implicit system =>
      buildProgram() *> IO.never.map(_ => ExitCode.Success)
    }
  }
}

// $COVERAGE-ON
