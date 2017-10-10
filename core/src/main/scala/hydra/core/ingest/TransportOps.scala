package hydra.core.ingest

import akka.util.Timeout
import configs.syntax._
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.core.akka.InitializingActor.{InitializationError, Initialized}
import hydra.core.protocol._
import hydra.core.transport.AckStrategy.Explicit
import hydra.core.transport.{AckStrategy, HydraRecord}

import scala.concurrent.Future
import scala.concurrent.duration.{FiniteDuration, _}

/**
  * Encapsulates basic transport operations: Look up an existing transport and
  * transports a HydraRequest using the looked up transport.
  *
  * Also has logic for dealing with errors.
  *
  * Created by alexsilva on 5/27/17.
  */
trait TransportOps extends ConfigSupport with LoggingAdapter {
  this: Ingestor =>

  implicit val ec = context.dispatcher

  /**
    * Always override this with a def due to how Scala initializes val in subtraits.
    */
  def transportName: String

  lazy val transportPath = applicationConfig.get[String](s"transports.$transportName.path")
    .valueOrElse(s"/user/service/${transportName}_transport")

  private lazy val transportResolveTimeout = Timeout(applicationConfig
    .getOrElse[FiniteDuration](s"transports.$transportName.resolve-timeout", 5 seconds).value)

  lazy val transportActorFuture = context.actorSelection(transportPath).resolveOne()(transportResolveTimeout)

  /**
    * Overrides the init method to look up a transport
    */
  override def init: Future[HydraMessage] = {
    transportActorFuture
      .map(t => Initialized)
      .recover {
        case e => InitializationError(new IllegalArgumentException(s"[$thisActorName]: No transport found " +
          s" at $transportPath", e))
      }
  }

  def transport[K, V](record: HydraRecord[K, V]): IngestorStatus = {
    record.ackStrategy match {
      case AckStrategy.None =>
        transportActorFuture.foreach(_ ! Produce(record))
        IngestorCompleted

      case Explicit =>
        transportActorFuture.foreach(_ ! ProduceWithAck(record, self, sender))
        WaitingForAck
    }
  }
}
