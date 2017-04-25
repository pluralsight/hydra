package hydra.sandbox.app

import java.io.File

import com.typesafe.config.ConfigFactory
import hydra.core.app.HydraEntryPoint
import hydra.ingest.IngestionActors

/**
  * Created by alexsilva on 3/29/17.
  */
object HydraIngest extends HydraEntryPoint with IngestionActors {
  override def moduleName: String = "sandbox"

  override def config = rootConfig.withFallback(ConfigFactory.parseFile(new File("/etc/hydra/hydra-sandbox.conf")))

  buildContainer().start()
}
