package hydra.ingest.services

import akka.actor.Actor
import configs.syntax._
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.common.util.ActorUtils
import hydra.ingest.bootstrap.ClasspathIngestorLoader
import hydra.ingest.ingestors.IngestorInfo
import hydra.ingest.services.IngestorRegistrar.UnregisterAll
import hydra.ingest.services.IngestorRegistry.{RegisterWithClass, Unregister, Unregistered}

/**
  * This actor serves as an proxy between the handler registry
  * and the application.
  *
  * Created by alexsilva on 12/5/16.
  */
class IngestorRegistrar extends Actor with ConfigSupport with LoggingAdapter {

  val registry = context.actorSelection(applicationConfig.get[String]("ingest.ingestor-registry.path")
    .valueOrElse("/user/service/ingestor_registry"))

  private val pkgs = applicationConfig.get[List[String]]("ingest.classpath-scan").valueOrElse(List.empty)

  log.debug(s"Scanning package(s) [$pkgs].")

  lazy val ingestors = new ClasspathIngestorLoader(pkgs).ingestors.map(h => ActorUtils.actorName(h) -> h)

  override def receive = {
    case RegisterWithClass(group, name, clazz) =>
      registry ! RegisterWithClass(group, name, clazz)

    case UnregisterAll =>
      ingestors.foreach(h => registry ! Unregister(h._1))

    case Unregistered(name) =>
      log.info(s"Ingestor $name was removed from the registry.")

    case IngestorInfo(name, group, path, _) =>
      log.info(s"Ingestor $name [$group] is available at $path")
  }

  override def preStart(): Unit = {
    ingestors.foreach(h => registry ! RegisterWithClass(h._2, "global", Some(h._1)))
  }
}

object IngestorRegistrar {

  case object UnregisterAll

}
