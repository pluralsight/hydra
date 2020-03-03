package hydra.core.bootstrap

import akka.actor.Props

/**
  * A trait that can be extended by classes providing services (actors) to be loaded during
  * boot time for Hydra.
  *
  * The [[BootstrappingSupport]] trait scans the classpath for objects that extend this interface
  * and adds the services provided to the list of services to be managed by Hydra.
  *
  */
trait ServiceProvider {

  /**
    * @return The list of services to be instantiated.
    */
  def services: Seq[(String, Props)]
}
