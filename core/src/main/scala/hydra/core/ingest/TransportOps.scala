package hydra.core.ingest

import hydra.common.config.ConfigSupport
import ConfigSupport._
import hydra.common.logging.LoggingAdapter
import hydra.core.akka.InitializingActor.{InitializationError, Initialized}
import hydra.core.protocol._
import hydra.core.transport.{AckStrategy, HydraRecord}

import scala.concurrent.Future
import scala.concurrent.duration._

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

  override def initTimeout =
    applicationConfig.getDurationOpt(s"transports.$transportName.resolve-timeout").getOrElse(30 seconds)

  /**
    * Always override this with a def due to how Scala initializes val in subtraits.
    */
  def transportName: String

  private val transportPath = applicationConfig
    .getStringOpt(s"transports.$transportName.path")
    .getOrElse(
      s"/user/service/transport_registrar/${transportName}_transport"
    )

  lazy val transportActorFuture =
    context.actorSelection(transportPath).resolveOne()(initTimeout)

  /**
    * Overrides the init method to look up a transport
    */
  override def init: Future[HydraMessage] = {
    transportActorFuture
      .map { _ =>
        log.info("{}[{}] initialized", Seq(thisActorName, self.path): _*);
        Initialized
      }
      .recover {
        case e =>
          InitializationError(
            new IllegalArgumentException(
              s"[$thisActorName]: No transport found " +
                s" at $transportPath",
              e
            )
          )
      }
  }

  def transport[K, V](record: HydraRecord[K, V], ack: AckStrategy): Unit = {
    val supervisor = sender()
    transportActorFuture.foreach(_ ! Produce(record, supervisor, ack))
  }

}
