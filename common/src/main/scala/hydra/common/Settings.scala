package hydra.common

import com.typesafe.config.Config
import hydra.common.config.ConfigSupport
import hydra.common.config.ConfigSupport._

import scala.concurrent.duration._

class Settings(config: Config) {

  val IngestTopicName: String = "hydra-ingest"

  val SchemaMetadataRefreshInterval: FiniteDuration = config
    .getDurationOpt("schema.metadata.refresh.interval")
    .getOrElse(1.minute)

  val httpPort = config.getInt("http.port")

  val httpInterface = config.getString("http.interface")
}

object Settings extends ConfigSupport {
  val HydraSettings = new Settings(applicationConfig)
}
