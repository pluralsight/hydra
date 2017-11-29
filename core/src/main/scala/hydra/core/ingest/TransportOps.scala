package hydra.core.ingest

import akka.actor.{ActorRef, Scheduler}
import configs.syntax._
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.core.akka.InitializingActor.{InitializationError, Initialized}
import hydra.core.protocol._
import hydra.core.transport.{AckStrategy, HydraRecord}

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import akka.pattern.after

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

  private val transportPath = applicationConfig.get[String](s"transports.$transportName.path")
    .valueOrElse(s"/user/service/transport_registrar/${transportName}_transport")

  override def initTimeout = applicationConfig
    .getOrElse[FiniteDuration](s"transports.$transportName.resolve-timeout", 30 seconds).value


  lazy val transportActorFuture = retry(context.actorSelection(transportPath).resolveOne()(initTimeout),
    2.seconds, 15)(ec, context.system.scheduler)

  /**
    * Overrides the init method to look up a transport
    */
  override def init: Future[HydraMessage] = {
    transportActorFuture
      .map { _ =>
        log.info("{}[{}] initialized", Seq(thisActorName, self.path): _*); Initialized }
      .recover {
        case e => InitializationError(new IllegalArgumentException(s"[$thisActorName]: No transport found " +
          s" at $transportPath", e))
      }
  }

  def transport[K, V](record: HydraRecord[K, V], supervisor: ActorRef, ack: AckStrategy): Unit = {
    transportActorFuture.foreach(_ ! Produce(record, supervisor, ack))
  }

  /**
    * Given an operation that produces a T, returns a Future containing the result of T, unless an exception is thrown,
    * in which case the operation will be retried after _delay_ time, if there are more possible retries, which is configured through
    * the _retries_ parameter. If the operation does not succeed and there is no retries left, the resulting Future will contain the last failure.
    **/
  def retry[T](op: => Future[T], delay: FiniteDuration, retries: Int)(implicit ec: ExecutionContext, s: Scheduler): Future[T] =
    op recoverWith { case _ if retries > 0 => after(delay, s)(retry(op, delay, retries - 1)) }
}
