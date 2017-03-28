package hydra.ingest.services

import akka.actor.Actor
import configs.syntax._
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.common.util.ActorUtils
import hydra.ingest.bootstrap.ClasspathIngestorLoader
import hydra.ingest.services.IngestorRegistry.{RegisterWithClass, Unregister}

/**
  * This actor serves as an proxy between the handler registry
  * and the application.
  *
  * Created by alexsilva on 12/5/16.
  */
class IngestorRegistrar extends Actor with ConfigSupport with LoggingAdapter {

  val registry = context.actorSelection(applicationConfig.get[String]("ingest.transport_registry.path")
    .valueOrElse("/user/service/ingestor_registry"))

  private val pkgs = applicationConfig.get[List[String]]("ingest.classpath-scan").valueOrElse(List.empty)

  lazy val ingestors = new ClasspathIngestorLoader(pkgs).ingestors.map(h => ActorUtils.actorName(h) -> h)

  override def receive = {
    case RegisterWithClass(name, clazz) =>
      registry ! RegisterWithClass(name, clazz)

    case IngestorInfo(name, path, time) =>
      log.info("Transport '%s' registered at %s".format(name, path.toString))
  }

  override def postStop(): Unit = {
    ingestors.foreach(h => registry ! Unregister(h._1))
  }

  override def preStart(): Unit = {
    ingestors.foreach(h => registry ! RegisterWithClass(h._1, h._2))
  }
}
