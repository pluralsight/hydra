package hydra.ingest.services

import akka.actor.Actor
import configs.syntax._
import hydra.common.akka.ActorUtils
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.core.ingest.{ClasspathIngestorDiscovery, IngestorInfo}
import hydra.ingest.services.IngestorRegistry.{RegisterIngestor, UnregisterIngestor}

/**
  * This actor serves as an proxy between the handler registry
  * and the application.
  *
  * Created by alexsilva on 12/5/16.
  */
class IngestorRegistrar extends Actor with ConfigSupport with LoggingAdapter {

  val registry = context.actorSelection(applicationConfig.get[String]("ingest.transport_registry.path")
    .valueOrElse("/user/service/ingestor_registry"))

  private val pkgs = applicationConfig.get[List[String]]("transports.scan").valueOrElse(List.empty)

  lazy val transports = new ClasspathIngestorDiscovery(pkgs).ingestors.map(h => ActorUtils.actorName(h) -> h)

  override def receive = {
    case RegisterIngestor(name, clazz) =>
      registry ! RegisterIngestor(name, clazz)

    case IngestorInfo(name, path, time) =>
      log.info("Transport '%s' registered at %s".format(name, path.toString))
  }

  override def postStop(): Unit = {
    transports.foreach(h => registry ! UnregisterIngestor(h._1))
  }

  override def preStart(): Unit = {
    transports.foreach(h => registry ! RegisterIngestor(h._1, h._2))
  }
}
