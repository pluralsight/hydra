package hydra.core.ingest

import akka.actor.{Actor, OneForOneStrategy, SupervisorStrategy}
import hydra.core.akka.InitializingActor
import hydra.core.protocol._

import scala.concurrent.duration._

/**
  * Created by alexsilva on 12/1/15.
  */

trait Ingestor extends InitializingActor {

  override def initTimeout = 2.seconds

  override val baseReceive: Receive = {
    case Publish(_) =>
      log.info(s"Publish message was not handled by ${self}.  Will not join.")
      sender ! Ignore

    case Validate(_) =>
      sender ! ValidRequest

    case ProducerAck(supervisor, error) =>
      supervisor ! error.map(IngestorError(_)).getOrElse(IngestorCompleted)
  }

  override def initializationError(ex: Throwable): Receive = {
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

  override val supervisorStrategy = OneForOneStrategy() { case _ => SupervisorStrategy.Restart }
}
