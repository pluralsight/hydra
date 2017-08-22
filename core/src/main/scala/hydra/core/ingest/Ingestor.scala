package hydra.core.ingest

import akka.actor.{Actor, ActorRef, OneForOneStrategy, ReceiveTimeout, Stash, SupervisorStrategy}
import akka.pattern.pipe
import hydra.common.config.ActorConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.core.HydraException
import hydra.core.akka.ComposingReceive
import hydra.core.ingest.Ingestor.{IngestorInitializationError, IngestorInitialized}
import hydra.core.protocol._

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, _}

/**
  * Created by alexsilva on 12/1/15.
  */

trait Ingestor extends Actor with ActorConfigSupport with LoggingAdapter with ComposingReceive with Stash {

  protected val initTimeout = 2.seconds //the initialization timeout

  context.setReceiveTimeout(initTimeout)

  override def receive: Receive = waitingForInitialization

  /**
    * This method is called upon initialization.  It should return one of the ingestor initialization messages:
    * `IngestorInitialized` or `IngestorInitializationError`
    *
    * The default implementation returns IngestorInitialized.
    */
  def initIngestor: Future[HydraMessage] = Future.successful(IngestorInitialized)

  override def preStart(): Unit = {
    implicit val ec = context.dispatcher
    pipe(initIngestor) to self
  }

  def waitingForInitialization: Receive = {
    case Ingestor.IngestorInitialized =>
      cancelReceiveTimeout
      context.become(composedReceive)
      log.info("Ingestor {}[{}] initialized", Seq(thisActorName, self.path): _*)
      unstashAll()

    case err@Ingestor.IngestorInitializationError(ex) =>
      log.error(ex.getMessage)
      initError(err)

    case ReceiveTimeout =>
      log.error(s"Ingestor $thisActorName[${self.path}] did not initialize in $initTimeout")
      val err = IngestorInitializationException(self, s"Ingestor did not initialize in $initTimeout.")
      initError(IngestorInitializationError(err))

    case msg =>
      log.debug(s"$thisActorName received message $msg while not initialized; stashing.")
      stash()
  }

  override val baseReceive: Receive = {
    case Publish(_) =>
      log.info(s"Publish message was not handled by ${self}.  Will not join.")
      sender ! Ignore

    case Validate(_) =>
      sender ! ValidRequest

    case ProducerAck(supervisor, error) =>
      supervisor ! error.map(IngestorError(_)).getOrElse(IngestorCompleted)
  }

  private def initError(err: IngestorInitializationError) = {
    cancelReceiveTimeout
    unstashAll()
    context.system.eventStream.publish(err)
    context.become(initializationError(err.t))
  }

  private def initializationError(ex: Throwable): Receive = {
    case _ =>
      sender ! IngestorError(ex)
      ingestionError(ex)
  }


  def ingest(next: Actor.Receive) = compose(next)

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    try {
      message.foreach(m => ingestionError(HydraIngestionError(thisActorName, reason, m.toString)))
    }
    finally {
      super.preRestart(reason, message)
    }
  }

  def ingestionError(error: HydraIngestionError): Unit = {
    log.error(s"Ingestion error: $error")
  }


  def ingestionError(error: Throwable): Unit = {
    log.error(s"Ingestion error: $error")
  }

  private def cancelReceiveTimeout = {
    context.setReceiveTimeout(Duration.Undefined)
    unstashAll()
  }

  override val supervisorStrategy = OneForOneStrategy() { case _ => SupervisorStrategy.Restart }
}

object Ingestor {

  /**
    * Signals this ingestor when to switch away from the `init` receive.
    */
  case object IngestorInitialized extends HydraMessage

  case class IngestorInitializationError(t: Throwable) extends HydraMessage

}

@SerialVersionUID(1L)
class IngestorInitializationException(ingestor: ActorRef, message: String, cause: Throwable)
  extends HydraException(IngestorInitializationException.enrichedMessage(ingestor, message), cause) {
  def getActor: ActorRef = ingestor
}

object IngestorInitializationException {
  private def enrichedMessage(actor: ActorRef, message: String) =
    Option(actor).map(a => s"${a.path}: $message").getOrElse(message)

  private[ingest] def apply(actor: ActorRef, message: String, cause: Throwable = null) =
    new IngestorInitializationException(actor, message, cause)

  def unapply(ex: IngestorInitializationException): Option[(ActorRef, String, Throwable)] =
    Some((ex.getActor, ex.getMessage, ex.getCause))
}