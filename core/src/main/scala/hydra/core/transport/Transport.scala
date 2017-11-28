package hydra.core.transport

import akka.actor.Actor
import hydra.common.config.ConfigSupport

/**
  * Just a marker trait that facilitates the dynamic loading of transports from the classpath at boot up time.
  */
trait Transport extends Actor with ConfigSupport