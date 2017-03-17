package hydra.ingest.ws

/**
  * Created by alexsilva on 3/10/17.
  */

import akka.actor.Actor
import hydra.common.logging.LoggingAdapter
import hydra.core.ingest.HydraRequestMedatata
import hydra.ingest.ws.IngestionSocketActor1._
import hydra.ingest.ws.chat._

class IngestionSocketActor1 extends Actor with LoggingAdapter {

  private var metadata: Map[String, HydraRequestMedatata] = Map.empty[String, HydraRequestMedatata]

  override def receive: Receive = {
    case SocketStarted(name) =>
      broadcast(SystemMessage(s"User $name joined channel..."))

    case SocketEnded(name) =>
      broadcast(SystemMessage(s"User $name left channel"))

    case msg: IncomingMessage =>
      parse(msg.message)
      broadcast(msg)
  }

  def parse(msg: String) = {
    msg match {
      case SetPattern(set, key, value) =>
        log.debug(s"Setting property ${key.toUpperCase} to $value")
        metadata += key.toUpperCase -> HydraRequestMedatata(key.toUpperCase, value)
    }
  }

  def broadcast(message: SocketMessage): Unit = {}

}

object IngestionSocketActor1 {
  private val SetPattern = "(--set )(.*)=(.*)".r

}
