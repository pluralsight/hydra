package hydra.core.bootstrap

import hydra.common.config.ConfigSupport
import org.reflections.Reflections
import org.reflections.scanners.SubTypesScanner
import org.reflections.util.ConfigurationBuilder

object ReflectionsWrapper extends ConfigSupport {

  import ConfigSupport._

  private[bootstrap] val scanPkgs = "hydra" +: applicationConfig
    .getStringListOpt("scan-packages").getOrElse(List.empty)

  private def reflectionsCfg =
    new ConfigurationBuilder()
      .forPackages(scanPkgs: _*)
      .addScanners(new SubTypesScanner)
      .useParallelExecutor()

  private var _reflections = new Reflections(reflectionsCfg)

  val reflections = _reflections

  //for testing only
  def rescan(): Unit = _reflections = new Reflections(reflectionsCfg)
}
