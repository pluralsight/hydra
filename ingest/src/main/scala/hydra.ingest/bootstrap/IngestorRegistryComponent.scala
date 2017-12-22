package hydra.ingest.bootstrap

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import hydra.common.config.ConfigSupport
import hydra.common.util.ActorUtils
import hydra.ingest.bootstrap.HydraIngestorRegistry.registryPath
import hydra.ingest.services.IngestorRegistry
import hydra.ingest.services.IngestorRegistry.{FindByName, LookupResult}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by alexsilva on 2/21/17.
  */
trait IngestorRegistryComponent {
  def ingestorRegistry: Future[ActorRef]
}

trait HydraIngestorRegistry extends IngestorRegistryComponent {

  import scala.concurrent.duration._

  implicit val system: ActorSystem

  lazy val ingestorRegistry: Future[ActorRef] = {
    implicit val registryLookUptimeout = Timeout(10 seconds)
    system.actorSelection(registryPath).resolveOne()
  }


  def lookupIngestor(name: String)(implicit ec: ExecutionContext): Future[LookupResult] = {
    implicit val registryLookUptimeout = Timeout(10 seconds)
    ingestorRegistry.flatMap(_ ? FindByName(name)).mapTo[LookupResult]
  }

}

object HydraIngestorRegistry extends ConfigSupport {

  import configs.syntax._

  //for testing
  val registryPath = applicationConfig.get[String]("ingest.ingestor-registry.path")
    .valueOrElse(s"/user/service/${ActorUtils.actorName(classOf[IngestorRegistry])}")
}
