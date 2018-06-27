package hydra.ingest.app

import configs.syntax._
import hydra.core.bootstrap.BootstrappingSupport
import kamon.Kamon
import kamon.prometheus.PrometheusReporter

// $COVERAGE-OFF$Disabling highlighting by default until a workaround for https://issues.scala-lang.org/browse/SI-8596 is found
object Main extends App with BootstrappingSupport {
  val enablePrometheus = applicationConfig.get[Boolean]("monitoring.prometheus.enable").valueOrElse(true)
  if (enablePrometheus) {
    Kamon.addReporter(new PrometheusReporter())
  }

  containerService.start()
}

// $COVERAGE-ON
