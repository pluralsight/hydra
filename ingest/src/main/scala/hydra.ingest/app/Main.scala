package hydra.ingest.app

import hydra.core.bootstrap.BootstrappingSupport

// $COVERAGE-OFF$Disabling highlighting by default until a workaround for https://issues.scala-lang.org/browse/SI-8596 is found
object Main extends App with BootstrappingSupport {
  containerService.start()
}

// $COVERAGE-ON
