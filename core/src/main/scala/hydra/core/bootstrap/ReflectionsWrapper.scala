package hydra.core.bootstrap

import hydra.common.config.ConfigSupport
import org.reflections.Reflections
import org.reflections.scanners.SubTypesScanner
import org.reflections.util.ConfigurationBuilder

object ReflectionsWrapper extends ConfigSupport {

  import configs.syntax._

  private[bootstrap] val scanPkgs = "hydra" +: applicationConfig
    .getOrElse[Seq[String]]("scan-packages", Seq.empty).value

  private val reflectionsCfg = new ConfigurationBuilder().forPackages(scanPkgs: _*)
    .addScanners(new SubTypesScanner).useParallelExecutor()

  val reflections = new Reflections(reflectionsCfg)
}
