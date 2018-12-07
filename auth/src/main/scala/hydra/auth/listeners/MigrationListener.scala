package hydra.auth.listeners

import com.github.vonnagy.service.container.listener.ContainerLifecycleListener
import com.github.vonnagy.service.container.service.ContainerService
import com.typesafe.config.ConfigFactory
import hydra.core.persistence.FlywaySupport

// $COVERAGE-OFF
// Skipping test, FlywaySupport is being tested elsewhere.
class MigrationListener extends ContainerLifecycleListener {

  override def onShutdown(container: ContainerService): Unit = {}

  override def onStartup(container: ContainerService): Unit = {
    val config = ConfigFactory.load().getConfig("pg-db")
    FlywaySupport.migrate(config)
  }
}

// $COVERAGE-ON
