package hydra.core.ingest

import akka.actor.{Actor, OneForOneStrategy, ReceiveTimeout, Stash, SupervisorStrategy}
import akka.pattern.pipe
import hydra.common.config.ActorConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.core.akka.ComposingReceive
import hydra.core.ingest.Ingestor.IngestorInitialized
import hydra.core.protocol._

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, _}
/**
  * Created by alexsilva on 12/1/15.
  */

trait Ingestor extends Actor with ActorConfigSupport with LoggingAdapter with ComposingReceive with Stash {

  //the initialization timeout
  private val initTimeout = 5.seconds
  context.setReceiveTimeout(initTimeout)

  override def receive: Receive = waitingForInitialization

  /**
    * This method is called upon initialization.  It should return one of the ingestor initialization messages:
    * `IngestorInitializationError` or `IngestorInitializationError`
    *
    * The default implementation returns IngestorInitialized.
    */
  private[ingest] def initIngestor: Future[HydraMessage] = Future.successful(IngestorInitialized)

  override def preStart(): Unit = {
    implicit val ec = context.dispatcher
    pipe(initIngestor) to self
  }

  def waitingForInitialization: Receive = {
    case Ingestor.IngestorInitialized =>
      cancelReceiveTimeout
      context.become(composedReceive)
      log.info("Ingestor {} initialized", thisActorName)
      unstashAll()

    case Ingestor.IngestorInitializationError(ex) =>
      log.error(ex.getMessage)
      initError(ex)

    case ReceiveTimeout => initError(new RuntimeException("Ingestor did not initialize in 5 seconds."))

    case msg =>
      log.debug(s"$thisActorName received message $msg while not initialized; stashing.")
      stash()
  }

  override val baseReceive: Receive = {
    case Publish(_) =>
      log.info(s"Publish message was not handled by ${self}.  Will not join.")

    case Validate(_) =>
      sender ! ValidRequest

    case Ingest(_) =>
      log.warn(s"Ingest message was not handled by ${self}.")
      sender ! IngestorCompleted

    case ProducerAck(supervisor, error) =>
      supervisor ! error.map(IngestorError(_)).getOrElse(IngestorCompleted)
  }

  private def initError(ex: Throwable) = {
    cancelReceiveTimeout
    context.become(initializationError(ex))
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

  override val supervisorStrategy =
    OneForOneStrategy() {
      case e: Exception => {
        SupervisorStrategy.Restart
      }
    }
}

object Ingestor {

  /**
    * Signals this ingestor when to switch away from the `init` receive.
    */
  case object IngestorInitialized extends HydraMessage

  case class IngestorInitializationError(t: Throwable) extends HydraMessage

}