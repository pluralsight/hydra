package hydra.ingest.bootstrap

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import hydra.common.config.ConfigSupport
import hydra.common.util.ActorUtils
import hydra.ingest.bootstrap.HydraIngestorRegistry.registryPath
import hydra.ingest.services.IngestorRegistry
import hydra.ingest.services.IngestorRegistry.{FindByName, LookupResult}

import scala.concurrent.Future

/**
  * Created by alexsilva on 2/21/17.
  */
trait IngestorRegistryComponent {
  def ingestorRegistry: Future[ActorRef]
}

trait HydraIngestorRegistry extends IngestorRegistryComponent {

  import scala.concurrent.duration._

  implicit val system: ActorSystem

  implicit val timeout = Timeout(500 millis)
  implicit val ec = system.dispatcher

  val ingestorRegistry: Future[ActorRef] = system.actorSelection(registryPath).resolveOne()

  def lookupIngestor(name: String): Future[LookupResult] =
    ingestorRegistry.flatMap(_ ? FindByName(name)).mapTo[LookupResult]
}

object HydraIngestorRegistry extends ConfigSupport {

  import configs.syntax._

  val registryPath = applicationConfig.get[String]("ingest.ingestor-registry.path")
    .valueOrElse(s"/user/service/${ActorUtils.actorName(classOf[IngestorRegistry])}")

}