package hydra.core.ingest

import akka.actor.{Actor, OneForOneStrategy, SupervisorStrategy}
import hydra.common.config.ActorConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.core.akka.ComposingReceive
import hydra.core.protocol._

/**
  * Created by alexsilva on 12/1/15.
  */

trait Ingestor extends Actor with ActorConfigSupport with LoggingAdapter with ComposingReceive {

  override val baseReceive: Receive = {
    case Publish(request) =>
      log.info(s"Publish message was not handled by ${self}.  Will not join.")

    case Validate(request) =>
      sender ! ValidRequest


    case Ingest(request) =>
      log.warn(s"Ingest message was not handled by ${self}.")
      sender ! IngestorCompleted

    case ProducerAck(supervisor, error) =>
      supervisor ! error.map(IngestorError(_)).getOrElse(IngestorCompleted)
  }


  def ingest(next: Actor.Receive) = compose(next)

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    try {
      val payload = message match {
        case Some(msg) => msg.toString //todo: create json
        case None => "No message"
      }
      ingestionError(HydraIngestionError(thisActorName, reason, payload))
    }
    catch {
      case e: Exception => log.error("Unable to send message in error to Kafka", e)
    }
  }

  def ingestionError(error: HydraIngestionError): Unit = {
    log.error(s"Ingestion error: $error")
  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case e: Exception => {
        SupervisorStrategy.Restart
      }
    }
}
