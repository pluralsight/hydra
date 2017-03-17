package hydra.ingest.ws

/**
  * Created by alexsilva on 3/10/17.
  */

import akka.actor.{Actor, ActorRef}
import hydra.common.logging.LoggingAdapter
import hydra.ingest.ws.IngestionSocketActor._

class IngestionSocketActor(ref:ActorRef) extends Actor with LoggingAdapter {

  private var ingestionMetadata: Map[String, Any] = Map.empty[String, Any]

  override def receive: Receive = {
    case SocketStarted(name) =>
      log.info(s"User $name started a web socket.")

    case SocketEnded(name) => context.stop(self)

    case msg: IncomingMessage =>
      parse(msg.message)
      sender ! "OK"
  }

  def parse(msg: String) = {
    msg match {
      case SetPattern(set, key, value) =>
        log.debug(s"Setting property ${key.toUpperCase} to $value")
        ingestionMetadata += key.toUpperCase -> value
    }


  }

  def broadcast(message: SocketMessage): Unit = {}

}

object IngestionSocketActor {
  private val SetPattern = "(--set )(.*)=(.*)".r

}