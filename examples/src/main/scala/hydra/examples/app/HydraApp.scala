package hydra.examples.app

import java.io.File

import com.typesafe.config.ConfigFactory
import hydra.core.app.HydraEntryPoint
import hydra.ingest.IngestionActors

/**
  * Created by alexsilva on 3/29/17.
  */
object HydraApp extends HydraEntryPoint with IngestionActors {
  override def moduleName: String = "examples"

  override def config = rootConfig.withFallback(ConfigFactory.parseFile(new File("/etc/hydra/hydra-example.conf")))

  buildContainer().start()
}
