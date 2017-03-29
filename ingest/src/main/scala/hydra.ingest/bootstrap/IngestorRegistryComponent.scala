package hydra.ingest.bootstrap

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.{after, ask}
import akka.util.Timeout
import hydra.common.config.ConfigSupport
import hydra.common.util.ActorUtils
import hydra.ingest.bootstrap.HydraIngestorRegistry.registryPath
import hydra.ingest.services.IngestorRegistry
import hydra.ingest.services.IngestorRegistry.{FindByName, LookupResult}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

/**
  * Created by alexsilva on 2/21/17.
  */
trait IngestorRegistryComponent {
  def ingestorRegistry: Future[ActorRef]
}

trait HydraIngestorRegistry extends IngestorRegistryComponent {

  import scala.concurrent.duration._

  implicit val system: ActorSystem

  implicit val timeout = Timeout(10 seconds)
  implicit val ec = system.dispatcher

  val ingestorRegistry: Future[ActorRef] =
    retry(system.actorSelection(registryPath).resolveOne(), RetryDelays.withDefault(List(1.second), 10, 1.second))

  def lookupIngestor(name: String): Future[LookupResult] =
    ingestorRegistry.flatMap(_ ? FindByName(name)).mapTo[LookupResult]

  def retry[T](f: => Future[T], delays: Seq[FiniteDuration]): Future[T] = {
    f recoverWith { case _ if delays.nonEmpty => after(delays.head, system.scheduler)(retry(f, delays.tail)) }
  }
}

object HydraIngestorRegistry extends ConfigSupport {

  import configs.syntax._

  val registryPath = applicationConfig.get[String]("ingest.ingestor-registry.path")
    .valueOrElse(s"/user/service/${ActorUtils.actorName(classOf[IngestorRegistry])}")

}

object RetryDelays {
  def withDefault(delays: List[FiniteDuration], retries: Int, default: FiniteDuration) = {
    if (delays.length > retries) delays take retries
    else delays ++ List.fill(retries - delays.length)(default)
  }

  def withJitter(delays: Seq[FiniteDuration], maxJitter: Double, minJitter: Double): Seq[Duration] =
    delays.map(_ * (minJitter + (maxJitter - minJitter) * Random.nextDouble))

  val fibonacci: Stream[FiniteDuration] = 0.seconds #:: 1.seconds #:: (fibonacci zip fibonacci.tail).map { t => t._1 + t._2 }
}