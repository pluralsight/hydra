package hydra.ingest.services

import akka.actor.Actor
import hydra.common.config.ConfigSupport
import ConfigSupport._
import hydra.common.logging.LoggingAdapter
import hydra.common.util.ActorUtils
import hydra.ingest.IngestorInfo
import hydra.ingest.bootstrap.ClasspathHydraComponentLoader
import hydra.ingest.services.IngestorRegistrar.UnregisterAll
import hydra.ingest.services.IngestorRegistry.{
  RegisterWithClass,
  Unregister,
  Unregistered
}

/**
  * This actor serves as an proxy between the handler registry
  * and the application.
  *
  * Created by alexsilva on 12/5/16.
  */
class IngestorRegistrar extends Actor with ConfigSupport with LoggingAdapter {

  private val ingestorRegistry = context.actorSelection(
    applicationConfig
      .getStringOpt("ingest.ingestor-registry.path")
      .getOrElse("/user/service/ingestor_registry")
  )

  lazy val ingestors = ClasspathHydraComponentLoader.ingestors.map(h =>
    ActorUtils.actorName(h) -> h
  )

  override def receive = {
    case RegisterWithClass(group, name, clazz) =>
      ingestorRegistry ! RegisterWithClass(group, name, clazz)

    case UnregisterAll =>
      ingestors.foreach(h => ingestorRegistry ! Unregister(h._1))

    case Unregistered(name) =>
      log.info(s"Ingestor $name was removed from the registry.")

    case IngestorInfo(name, group, path, _) =>
      log.info(s"Ingestor $name [$group] is available at $path")
  }

  override def preStart(): Unit = {
    ingestors.foreach(h =>
      ingestorRegistry ! RegisterWithClass(h._2, "global", Some(h._1))
    )
  }
}

object IngestorRegistrar {

  case object UnregisterAll

}
