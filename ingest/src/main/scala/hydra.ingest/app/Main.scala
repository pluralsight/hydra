package hydra.ingest.app

import configs.syntax._
import hydra.common.logging.LoggingAdapter
import hydra.core.bootstrap.BootstrappingSupport
import kamon.Kamon
import kamon.prometheus.PrometheusReporter
import scala.concurrent.ExecutionContext.Implicits.global

// $COVERAGE-OFF$Disabling highlighting by default until a workaround for https://issues.scala-lang.org/browse/SI-8596 is found
object Main extends App with BootstrappingSupport with LoggingAdapter {
  try {

    val enablePrometheus = applicationConfig
      .get[Boolean]("monitoring.prometheus.enable")
      .valueOrElse(true)
    if (enablePrometheus) {
      Kamon.addReporter(new PrometheusReporter())
    }

    containerService.start()

  } catch {
    case e: Throwable => {
      log.error("Unhandled exception.  Shutting down actor system.", e)
      Kamon
        .stopAllReporters()
        .map(_ => containerService.shutdown())
        .onComplete(throw e)
    }
  }
}

// $COVERAGE-ON
