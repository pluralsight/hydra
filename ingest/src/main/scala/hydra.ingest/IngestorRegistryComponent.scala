package hydra.ingest

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import hydra.common.config.ConfigSupport
import hydra.common.util.ActorUtils
import hydra.ingest.services.IngestorRegistry
import hydra.ingest.services.IngestorRegistry.{IngestorLookupResult, Lookup}

import scala.concurrent.Future

/**
  * Created by alexsilva on 2/21/17.
  */
trait IngestorRegistryComponent {
  def ingestorRegistry: Future[ActorRef]

  implicit val system: ActorSystem
}

trait HydraIngestorRegistry extends IngestorRegistryComponent with ConfigSupport {

  import configs.syntax._

  import scala.concurrent.duration._

  implicit val timeout = Timeout(500 millis)
  implicit val ec = system.dispatcher
  //
  //  val ingestorRegistry: Future[ActorRef] = ServicesManager.findService("ingestor_registry",
  //    "service-container/user/service/")

  val registryPath = applicationConfig.get[String]("ingest.ingestor_registry.path")
    .valueOrElse(s"/user/service/${ActorUtils.actorName(classOf[IngestorRegistry])}")

  val ingestorRegistry: Future[ActorRef] = system.actorSelection(registryPath).resolveOne()

  def lookupIngestor(name: String): Future[IngestorLookupResult] =
    ingestorRegistry.flatMap(_ ? Lookup(name)).mapTo[IngestorLookupResult]

}



