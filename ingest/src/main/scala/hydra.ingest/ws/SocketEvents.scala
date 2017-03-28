package hydra.ingest.ws


import akka.actor.ActorRef

/**
  * Created by alexsilva on 3/10/17.
  */

sealed trait SocketEvent

case class SocketStarted(name: String, ref: ActorRef) extends SocketEvent

case object SocketEnded extends SocketEvent

case class IncomingMessage(message: String) extends SocketEvent

case class OutgoingMessage(status: Int, message: String) extends SocketEvent
